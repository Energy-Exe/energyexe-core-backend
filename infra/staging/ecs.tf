# Staging backend: its own ECR repo + log group + task def + service, on the
# SHARED cluster (data.aws_ecs_cluster.shared). Adding a service to a cluster
# can't affect prod's service.

resource "aws_ecr_repository" "this" {
  name                 = local.name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "keep_last_5" {
  repository = aws_ecr_repository.this.name

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

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${local.name}"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "api" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([{
    name      = local.container_name
    image     = "${aws_ecr_repository.this.repository_url}:${var.image_tag}"
    essential = true

    # Same as prod: migrate, then ONE uvicorn worker. The nightly APScheduler
    # pipeline is disabled on staging (PIPELINE_DAILY_ENABLED=false), so a
    # single small task is plenty.
    command = [
      "sh", "-c",
      "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port ${local.container_port} --workers 1"
    ]

    portMappings = [{
      containerPort = local.container_port
      protocol      = "tcp"
    }]

    # Staging skips Valkey entirely (no REDIS_URL): the report cache + rate
    # limiter degrade gracefully to no-cache / fail-open.
    environment = concat(
      [
        { name = "DEBUG", value = "false" },
        { name = "LOG_LEVEL", value = "INFO" },
        { name = "SENTRY_ENVIRONMENT", value = "staging" },
        # Pipeline OFF on staging — no scheduler, no heavy nightly load.
        { name = "PIPELINE_DAILY_ENABLED", value = "false" },
        { name = "CDSAPI_URL", value = "https://cds.climate.copernicus.eu/api" },
        { name = "AWS_DEFAULT_REGION", value = var.region },
        { name = "S3_BUCKET_NAME", value = var.s3_bucket_name },
        { name = "EMAILS_FROM_EMAIL", value = "noreply@updates.energyexe.com" },
        { name = "EMAILS_FROM_NAME", value = "Energyexe (staging)" },
        { name = "SUPPORT_EMAIL", value = "hello@energyexe.com" },
      ],
      # Staging frontends run on Vercel. Set these to the Vercel staging URLs via
      # tfvars once known; unset is fine for API testing. BACKEND_CORS_ORIGINS
      # must be a JSON-array string, e.g. '["https://staging-xyz.vercel.app"]'.
      var.client_portal_url == "" ? [] : [{ name = "CLIENT_PORTAL_URL", value = var.client_portal_url }],
      var.admin_portal_url == "" ? [] : [{ name = "ADMIN_PORTAL_URL", value = var.admin_portal_url }],
      var.cors_origins == "" ? [] : [{ name = "BACKEND_CORS_ORIGINS", value = var.cors_origins }],
    )

    secrets = concat(
      [
        { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn },
        { name = "SECRET_KEY", valueFrom = aws_secretsmanager_secret.secret_key.arn },
      ],
      [for env_name in sort(keys(local.reused_secret_env)) :
        { name = env_name, valueFrom = data.aws_secretsmanager_secret.reused[local.reused_secret_env[env_name]].arn }
      ],
      # Only wired once the staging sentry-dsn secret is populated — an empty
      # secret would stop the task from starting.
      var.backend_sentry_dsn_enabled ? [
        { name = "SENTRY_DSN", valueFrom = aws_secretsmanager_secret.sentry_dsn.arn }
      ] : []
    )

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.this.name
        awslogs-region        = var.region
        awslogs-stream-prefix = local.container_name
      }
    }
  }])
}

resource "aws_ecs_service" "api" {
  name            = local.name
  cluster         = data.aws_ecs_cluster.shared.arn
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 1

  # On-demand FARGATE (not Spot): Spot would require associating a FARGATE_SPOT
  # capacity provider with the SHARED cluster, which is prod-owned — we keep
  # staging from touching the cluster config at all. ~$12/mo more, prod-safe.
  launch_type = "FARGATE"

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  health_check_grace_period_seconds  = 300

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets         = data.aws_subnets.default.ids
    security_groups = [aws_security_group.service.id]
    # Public IP needed for ECR pulls + external data APIs (no NAT gateway).
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = local.container_name
    container_port   = local.container_port
  }
}
