# Self-hosted GlitchTip — open-source, Sentry-API-compatible error tracker.
#
# Runs as a second Fargate service in the same `energyexe` cluster, behind the
# SAME ALB via host-based routing (errors.energyexe.com). The backend reports
# errors to it with the Sentry SDK (see app/core/observability.py).
#
# One task, four containers: `migrate` (runs DB migrations to completion, then
# exits; web+worker gate on it via dependsOn SUCCESS) + `web` (UI + ingest,
# :8000) + `worker` (celery + beat) + `redis` (ephemeral sidecar broker/cache;
# durable data lives in Postgres). GlitchTip v6's default ./bin/start.sh is
# web-only, so migrations need the dedicated migrate container. Postgres is a NEW
# `glitchtip` database on the existing energyexedb RDS instance (see
# infra/README.md for the CREATE DATABASE runbook).
#
# Two-phase apply, mirroring the API cert flow (DNS is at hyp.net, not Route53):
#   Phase 1 — set glitchtip_domain, leave glitchtip_certificate_arn="".
#             Apply creates the ACM cert + secret containers + SG + IAM. Add the
#             validation CNAME (glitchtip_acm_validation_records output) at
#             hyp.net, wait for ISSUED, create the RDS db, and populate the 3
#             secrets — all while the cert validates.
#   Phase 2 — set glitchtip_certificate_arn to the issued ARN. Apply creates the
#             target group, listener wiring, task definition, and service. Add
#             the errors -> ALB CNAME (glitchtip_cname_target) at hyp.net.
#
# NOTE: the listener rule attaches to the API's HTTPS listener, so the API cert
# (var.certificate_arn) must already exist.

locals {
  glitchtip_name    = "energyexe-glitchtip"
  glitchtip_base    = var.glitchtip_domain != "" ? 1 : 0          # phase 1: prep
  glitchtip_serving = var.glitchtip_certificate_arn != "" ? 1 : 0 # phase 2: serve

  # redis comes from the ECR Public Gallery (a Docker Official Images mirror):
  # unlimited, no-auth pulls to any AWS region, so no Docker Hub account or
  # pull-through cache is needed. GlitchTip is Docker-Hub-only, so it's mirrored
  # once into the private ECR repo below (see README) — the task pulls that.
  redis_image = "public.ecr.aws/docker/library/redis:7-alpine"

  # Plain (non-secret) env shared by the web + worker containers. No count refs,
  # so this is always safe to evaluate.
  glitchtip_env = [
    { name = "GLITCHTIP_DOMAIN", value = "https://${var.glitchtip_domain}" },
    { name = "DEFAULT_FROM_EMAIL", value = "noreply@updates.energyexe.com" },
    # Lock the instance down: no public sign-ups. Create the first user via the
    # brief open-registration window or the GlitchTip shell (see README).
    { name = "ENABLE_OPEN_USER_REGISTRATION", value = "false" },
    # Redis sidecar in the same task -> reachable on localhost (awsvpc shares
    # the network namespace across containers).
    { name = "REDIS_URL", value = "redis://localhost:6379/0" },
    { name = "PORT", value = "8000" },
    # Cap storage growth: drop events older than 90 days.
    { name = "GLITCHTIP_MAX_EVENT_LIFE_DAYS", value = "90" },
  ]
}

# --- TLS certificate for errors.energyexe.com (phase 1) ---

resource "aws_acm_certificate" "glitchtip" {
  count             = local.glitchtip_base
  domain_name       = var.glitchtip_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# --- Secret CONTAINERS (phase 1) — values set out-of-band, never in TF state.
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/glitchtip/secret-key --secret-string "$(openssl rand -hex 32)"
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/glitchtip/database-url \
#     --secret-string 'postgres://glitchtip:PASS@energyexedb.<id>.eu-north-1.rds.amazonaws.com:5432/glitchtip'
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/glitchtip/email-url \
#     --secret-string 'smtp://resend:<RESEND_API_KEY>@smtp.resend.com:587'
#
# DATABASE_URL uses the plain postgres:// scheme (GlitchTip is Django/psycopg,
# NOT asyncpg).

resource "aws_secretsmanager_secret" "glitchtip_secret_key" {
  count       = local.glitchtip_base
  name        = "energyexe/glitchtip/secret-key"
  description = "Django SECRET_KEY for GlitchTip"
}

resource "aws_secretsmanager_secret" "glitchtip_database_url" {
  count       = local.glitchtip_base
  name        = "energyexe/glitchtip/database-url"
  description = "GlitchTip Postgres URL (postgres://glitchtip:...@energyexedb:5432/glitchtip)"
}

resource "aws_secretsmanager_secret" "glitchtip_email_url" {
  count       = local.glitchtip_base
  name        = "energyexe/glitchtip/email-url"
  description = "SMTP EMAIL_URL for GlitchTip alert emails (Resend)"
}

# --- Private ECR repo for the GlitchTip image (phase 1) ---
#
# GlitchTip publishes only to Docker Hub, so mirror it here once (no Docker Hub
# account needed — one anonymous pull is well under any rate limit), then the
# task pulls from private ECR. Repeat the mirror on each GlitchTip upgrade:
#
#   docker pull glitchtip/glitchtip:<tag>
#   docker tag  glitchtip/glitchtip:<tag> <repo_url>:<tag>
#   aws ecr get-login-password --profile energyexe --region eu-north-1 \
#     | docker login --username AWS --password-stdin <account>.dkr.ecr.eu-north-1.amazonaws.com
#   docker push <repo_url>:<tag>
#
# (repo_url = the glitchtip_ecr_repository_url output)
resource "aws_ecr_repository" "glitchtip" {
  count                = local.glitchtip_base
  name                 = local.glitchtip_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "glitchtip_keep_last_5" {
  count      = local.glitchtip_base
  repository = aws_ecr_repository.glitchtip[0].name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# --- Security group (phase 1): only reachable from the ALB; talks to RDS ---

resource "aws_security_group" "glitchtip" {
  count       = local.glitchtip_base
  name_prefix = "${local.glitchtip_name}-svc-"
  description = "Fargate tasks for GlitchTip"
  vpc_id      = data.aws_vpc.default.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "glitchtip_from_alb" {
  count                        = local.glitchtip_base
  security_group_id            = aws_security_group.glitchtip[0].id
  description                  = "Web traffic from ALB"
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = 8000
  to_port                      = 8000
  ip_protocol                  = "tcp"
}

# Outbound open: Docker Hub image pulls, Secrets Manager, CloudWatch Logs,
# Resend SMTP, and the RDS instance.
resource "aws_vpc_security_group_egress_rule" "glitchtip_all" {
  count             = local.glitchtip_base
  security_group_id = aws_security_group.glitchtip[0].id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# Let GlitchTip reach Postgres on the existing RDS SG (SG-to-SG, like the API).
resource "aws_vpc_security_group_ingress_rule" "rds_from_glitchtip" {
  count                        = local.glitchtip_base
  security_group_id            = var.rds_security_group_id
  description                  = "Postgres from GlitchTip ECS service"
  referenced_security_group_id = aws_security_group.glitchtip[0].id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# --- Logs + execution role (phase 1) ---

resource "aws_cloudwatch_log_group" "glitchtip" {
  count             = local.glitchtip_base
  name              = "/ecs/${local.glitchtip_name}"
  retention_in_days = 30
}

resource "aws_iam_role" "glitchtip_execution" {
  count              = local.glitchtip_base
  name               = "${local.glitchtip_name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "glitchtip_execution_managed" {
  count      = local.glitchtip_base
  role       = aws_iam_role.glitchtip_execution[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "glitchtip_execution_secrets" {
  count = local.glitchtip_base
  name  = "read-glitchtip-secrets"
  role  = aws_iam_role.glitchtip_execution[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["secretsmanager:GetSecretValue"]
      Resource = [
        aws_secretsmanager_secret.glitchtip_secret_key[0].arn,
        aws_secretsmanager_secret.glitchtip_database_url[0].arn,
        aws_secretsmanager_secret.glitchtip_email_url[0].arn,
      ]
    }]
  })
}

# --- ALB wiring (phase 2): host rule on the existing HTTPS listener ---

resource "aws_lb_target_group" "glitchtip" {
  count       = local.glitchtip_serving
  name        = "${local.glitchtip_name}-tg"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  # "/" returns 200 (or a redirect to the login page) on every GlitchTip
  # version, so it's a robust health signal without depending on a specific
  # health path. A lighter "/_health/" endpoint also exists if preferred.
  health_check {
    path                = "/"
    matcher             = "200-399"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 5
  }

  deregistration_delay = 30
}

# Add the errors.energyexe.com cert to the API's 443 listener (SNI), so TLS
# terminates cleanly for both hosts on the one ALB.
resource "aws_lb_listener_certificate" "glitchtip" {
  count           = local.glitchtip_serving
  listener_arn    = aws_lb_listener.https[0].arn
  certificate_arn = var.glitchtip_certificate_arn
}

resource "aws_lb_listener_rule" "glitchtip" {
  count        = local.glitchtip_serving
  listener_arn = aws_lb_listener.https[0].arn
  priority     = 100

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.glitchtip[0].arn
  }

  condition {
    host_header {
      values = [var.glitchtip_domain]
    }
  }
}

# --- Task definition + service (phase 2) ---

resource "aws_ecs_task_definition" "glitchtip" {
  count                    = local.glitchtip_serving
  family                   = local.glitchtip_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.glitchtip_task_cpu
  memory                   = var.glitchtip_task_memory
  execution_role_arn       = aws_iam_role.glitchtip_execution[0].arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    # Ephemeral broker/cache. --save "" + --appendonly no => no persistence;
    # GlitchTip's durable data is in Postgres, so a wiped Redis on restart is
    # fine.
    {
      name      = "redis"
      image     = local.redis_image
      essential = true
      command   = ["redis-server", "--save", "", "--appendonly", "no"]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.glitchtip[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "redis"
        }
      }
    },
    # Migrate: runs DB migrations to completion, then exits (essential=false).
    # web + worker gate on it via dependsOn SUCCESS, so the schema always exists
    # before they start — no startup race, and upgrades migrate automatically on
    # each deploy. GlitchTip v6's default ./bin/start.sh is web-only.
    {
      name        = "migrate"
      image       = "${aws_ecr_repository.glitchtip[0].repository_url}:${var.glitchtip_image_tag}"
      essential   = false
      command     = ["./bin/run-migrate.sh"]
      environment = local.glitchtip_env
      secrets = [
        { name = "SECRET_KEY", valueFrom = aws_secretsmanager_secret.glitchtip_secret_key[0].arn },
        { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.glitchtip_database_url[0].arn },
        { name = "EMAIL_URL", valueFrom = aws_secretsmanager_secret.glitchtip_email_url[0].arn },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.glitchtip[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "migrate"
        }
      }
    },
    # Web: UI + Sentry ingest endpoint (granian on $PORT = 8000).
    {
      name         = "web"
      image        = "${aws_ecr_repository.glitchtip[0].repository_url}:${var.glitchtip_image_tag}"
      essential    = true
      portMappings = [{ containerPort = 8000, protocol = "tcp" }]
      dependsOn = [
        { containerName = "migrate", condition = "SUCCESS" },
        { containerName = "redis", condition = "START" },
      ]
      environment = local.glitchtip_env
      secrets = [
        { name = "SECRET_KEY", valueFrom = aws_secretsmanager_secret.glitchtip_secret_key[0].arn },
        { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.glitchtip_database_url[0].arn },
        { name = "EMAIL_URL", valueFrom = aws_secretsmanager_secret.glitchtip_email_url[0].arn },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.glitchtip[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "web"
        }
      }
    },
    # Worker: celery + beat. Confirm this command against GlitchTip's current
    # docker-compose at apply time (it has historically been this script).
    {
      name      = "worker"
      image     = "${aws_ecr_repository.glitchtip[0].repository_url}:${var.glitchtip_image_tag}"
      essential = true
      command   = ["./bin/run-celery-with-beat.sh"]
      dependsOn = [
        { containerName = "migrate", condition = "SUCCESS" },
        { containerName = "redis", condition = "START" },
      ]
      environment = local.glitchtip_env
      secrets = [
        { name = "SECRET_KEY", valueFrom = aws_secretsmanager_secret.glitchtip_secret_key[0].arn },
        { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.glitchtip_database_url[0].arn },
        { name = "EMAIL_URL", valueFrom = aws_secretsmanager_secret.glitchtip_email_url[0].arn },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.glitchtip[0].name
          awslogs-region        = var.region
          awslogs-stream-prefix = "worker"
        }
      }
    },
  ])
}

resource "aws_ecs_service" "glitchtip" {
  count           = local.glitchtip_serving
  name            = local.glitchtip_name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.glitchtip[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # Single task (like the API): a deploy briefly drops the tracker. Acceptable
  # for internal tooling and avoids two celery-beat schedulers double-running.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  # The migrate container runs first (web waits on it via dependsOn SUCCESS), so
  # allow time for the first full migration + web startup before the ALB can kill
  # the task.
  health_check_grace_period_seconds = 300

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets         = data.aws_subnets.default.ids
    security_groups = [aws_security_group.glitchtip[0].id]
    # Public subnets, no NAT: a public IP is needed for the Docker Hub pull and
    # Resend SMTP egress. Inbound stays ALB-only via the security group.
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.glitchtip[0].arn
    container_name   = "web"
    container_port   = 8000
  }
}

# --- Outputs ---

output "glitchtip_acm_validation_records" {
  description = "Add these CNAME record(s) at hyp.net to validate the GlitchTip certificate."
  value = local.glitchtip_base == 0 ? [] : [
    for o in aws_acm_certificate.glitchtip[0].domain_validation_options : {
      add_cname_named = o.resource_record_name
      with_value      = o.resource_record_value
    }
  ]
}

output "glitchtip_acm_certificate_arn" {
  description = "Set as glitchtip_certificate_arn in terraform.tfvars once ISSUED, then re-apply (phase 2)."
  value       = local.glitchtip_base == 0 ? "" : aws_acm_certificate.glitchtip[0].arn
}

output "glitchtip_cname_target" {
  description = "After phase 2, CNAME glitchtip_domain (errors.energyexe.com) to this at hyp.net."
  value       = aws_lb.this.dns_name
}

output "glitchtip_ecr_repository_url" {
  description = "Mirror the GlitchTip image here (docker pull glitchtip/glitchtip:<tag> -> push)."
  value       = local.glitchtip_base == 0 ? "" : aws_ecr_repository.glitchtip[0].repository_url
}
