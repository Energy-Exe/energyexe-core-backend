resource "aws_ecr_repository" "this" {
  name                 = local.name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "keep_last_10" {
  repository = aws_ecr_repository.this.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${local.name}"
  retention_in_days = 30
}

resource "aws_ecs_cluster" "this" {
  name = "energyexe"
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

    # Overrides the Dockerfile CMD for two reasons:
    #  - migrations run before the server starts (single task => no race)
    #  - exactly ONE uvicorn worker: APScheduler starts in each worker's
    #    lifespan, so the Dockerfile's `--workers 4` would run every cron 4x.
    command = [
      "sh", "-c",
      "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port ${local.container_port} --workers 1"
    ]

    portMappings = [{
      containerPort = local.container_port
      protocol      = "tcp"
    }]

    environment = concat(
      [
        { name = "DEBUG", value = "false" },
        { name = "LOG_LEVEL", value = "INFO" },
        { name = "PIPELINE_DAILY_ENABLED", value = tostring(var.pipeline_daily_enabled) },
        { name = "CDSAPI_URL", value = "https://cds.climate.copernicus.eu/api" },
        # Region for boto3; credentials come from the task role, not env keys.
        { name = "AWS_DEFAULT_REGION", value = var.region },
        { name = "S3_BUCKET_NAME", value = var.s3_bucket_name },
        { name = "EMAILS_FROM_EMAIL", value = "noreply@updates.energyexe.com" },
        { name = "EMAILS_FROM_NAME", value = "Energyexe" },
        { name = "SUPPORT_EMAIL", value = "hello@energyexe.com" },
        { name = "CLIENT_PORTAL_URL", value = "https://app.energyexe.com" },
        { name = "ADMIN_PORTAL_URL", value = "https://dashboard.energyexe.com" },
        # No password — access is gated by security group, and the endpoint is
        # only reachable inside the VPC. rediss:// because serverless
        # ElastiCache requires TLS.
        {
          name  = "REDIS_URL"
          value = "rediss://${aws_elasticache_serverless_cache.valkey.endpoint[0].address}:${aws_elasticache_serverless_cache.valkey.endpoint[0].port}"
        },
      ],
      # Unset by default: the app's built-in CORS list already covers the prod
      # and dev origins. If overridden it MUST be a JSON array string —
      # pydantic-settings json-parses List[str] env vars before any validator
      # runs, so a bare comma-separated value crashes the app at startup.
      var.cors_origins == "" ? [] : [{ name = "BACKEND_CORS_ORIGINS", value = var.cors_origins }]
    )

    secrets = concat(
      [
        { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn },
        { name = "SECRET_KEY", valueFrom = aws_secretsmanager_secret.secret_key.arn },
      ],
      [for env_name in sort(keys(local.extra_secrets)) :
        { name = env_name, valueFrom = aws_secretsmanager_secret.extra[env_name].arn }
      ]
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
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # Never run two tasks at once — a second task means a second APScheduler
  # double-firing every cron. Tradeoff: deploys have ~1-2 min of downtime
  # (old task stops, new one starts) instead of a rolling handover.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  # Migrations run before uvicorn binds, so give the task time before the
  # ALB health check can kill it.
  health_check_grace_period_seconds = 300

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets         = data.aws_subnets.default.ids
    security_groups = [aws_security_group.service.id]
    # Default-VPC public subnets, no NAT gateway: the task needs a public IP
    # for ECR pulls and the external data APIs. Inbound is still ALB-only
    # via the security group.
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = local.container_name
    container_port   = local.container_port
  }
}
