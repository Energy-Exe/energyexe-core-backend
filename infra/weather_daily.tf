# Daily ERA5 weather import — EventBridge -> one-off ECS Fargate task (EPR-121).
#
# WHY THIS EXISTS: nothing scheduled weather before. `weather_data` is filled
# by Copernicus CDS ERA5 downloads that were only ever run by hand (EC2 fleets,
# the admin "Weather data → Import" page), so the whole fleet's weather stopped
# at 2025-12-31 when the last backfill ended — and because the performance
# pipeline inner-joins generation with weather, every 2026 hour was silently
# dropped from power curves, ODI and the nightly detection.
#
# Same shape as pipeline_daily.tf: a separate task-definition family, the API's
# roles and security group, EventBridge as the clock, ECS task state changes as
# the failure signal. Runs at 01:30 UTC so the day is in the database before
# the 03:00 pipeline reads it. Each run imports ONE day, `today - WEATHER_LAG_DAYS`
# (ERA5T is published ~5-6 days behind real time), and records a
# `weather_import_jobs` row.
#
# Backfills reuse the same task definition with a command override, e.g.
#   --overrides '{"containerOverrides":[{"name":"weather","command":
#     ["python","scripts/jobs/run_weather_daily.py","--start","2026-01-01","--end","2026-03-01"]}]}'
# (see output "weather_run_task_command"). Add --windfarm-ids to scope a run to
# newly added farms — that shrinks the CDS bounding box to those farms.

locals {
  weather_enabled = local.pipeline_enabled
}

resource "aws_cloudwatch_log_group" "weather" {
  count             = local.weather_enabled
  name              = "/ecs/${local.name}-weather"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "weather" {
  count                    = local.weather_enabled
  family                   = "${local.name}-weather"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.weather_task_cpu
  memory                   = var.weather_task_memory

  execution_role_arn = aws_iam_role.execution.arn
  task_role_arn      = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([{
    name      = "weather"
    image     = "${aws_ecr_repository.this.repository_url}:${var.image_tag}"
    essential = true

    # No alembic here either — the API service owns migrations.
    command = ["python", "scripts/jobs/run_weather_daily.py"]

    environment = [
      { name = "LOG_LEVEL", value = "INFO" },
      { name = "SENTRY_ENVIRONMENT", value = "production" },
      { name = "PYTHONUNBUFFERED", value = "1" },
      { name = "CDSAPI_URL", value = "https://cds.climate.copernicus.eu/api" },
      # ERA5T lag: the day imported is today minus this many days.
      { name = "WEATHER_LAG_DAYS", value = tostring(var.weather_lag_days) },
      # Tell the GlitchTip cron monitor when to expect the check-in — derived
      # from the same variables as the rule below so they cannot drift.
      { name = "WEATHER_DAILY_HOUR", value = tostring(var.weather_daily_hour) },
      { name = "WEATHER_DAILY_MINUTE", value = tostring(var.weather_daily_minute) },
      { name = "DB_POOL_SIZE", value = "5" },
      { name = "DB_MAX_OVERFLOW", value = "5" },
      { name = "AWS_DEFAULT_REGION", value = var.region },
    ]

    secrets = concat(
      [
        { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn },
        { name = "CDSAPI_KEY", valueFrom = aws_secretsmanager_secret.extra["CDSAPI_KEY"].arn },
      ],
      var.backend_sentry_dsn_enabled ? [
        { name = "SENTRY_DSN", valueFrom = aws_secretsmanager_secret.backend_sentry_dsn.arn }
      ] : []
    )

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.weather[0].name
        awslogs-region        = var.region
        awslogs-stream-prefix = "weather"
      }
    }
  }])
}

# --- Schedule ---------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "weather_daily" {
  count               = local.weather_enabled
  name                = "${local.name}-weather-daily"
  description         = "Daily ERA5 weather import (one day, today minus the ERA5T lag) ahead of the 03:00 pipeline."
  schedule_expression = "cron(${var.weather_daily_minute} ${var.weather_daily_hour} * * ? *)"
  state               = var.weather_schedule_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "weather_daily" {
  count     = local.weather_enabled
  rule      = aws_cloudwatch_event_rule.weather_daily[0].name
  target_id = "weather-daily-task"
  arn       = aws_ecs_cluster.this.arn
  role_arn  = aws_iam_role.weather_events[0].arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.weather[0].arn
    task_count          = 1
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.service.id]
      assign_public_ip = true
    }
  }

  # Retries the RunTask invocation only, never a run that started and failed
  # (a failed day is re-imported by the next night's completeness check anyway).
  retry_policy {
    maximum_retry_attempts       = 3
    maximum_event_age_in_seconds = 3600
  }
}

# --- IAM: let EventBridge start the task ------------------------------------
resource "aws_iam_role" "weather_events" {
  count = local.weather_enabled
  name  = "${local.name}-weather-events"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "weather_events" {
  count = local.weather_enabled
  name  = "run-task"
  role  = aws_iam_role.weather_events[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecs:RunTask"]
        Resource = "${aws_ecs_task_definition.weather[0].arn_without_revision}:*"
      },
      {
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = [aws_iam_role.task.arn, aws_iam_role.execution.arn]
        Condition = { StringEquals = { "iam:PassedToService" = "ecs-tasks.amazonaws.com" } }
      },
    ]
  })
}

# --- Failure detection ------------------------------------------------------
# Exit code 1 = a day failed / job crashed; abnormal stops (OOM, image pull,
# capacity) have no exitCode, so match on stopCode too (same reasoning as the
# pipeline rule). The SNS topic policy in pipeline_daily.tf already allows
# EventBridge to publish.
resource "aws_cloudwatch_event_rule" "weather_task_failed" {
  count       = local.weather_enabled
  name        = "${local.name}-weather-task-failed"
  description = "Daily weather import task stopped abnormally or exited non-zero."

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      lastStatus        = ["STOPPED"]
      clusterArn        = [aws_ecs_cluster.this.arn]
      taskDefinitionArn = [{ prefix = "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task-definition/${local.name}-weather" }]
      "$or" = [
        { stopCode = [{ "anything-but" = ["EssentialContainerExited"] }] },
        { containers = { exitCode = [{ "anything-but" = [0] }] } },
      ]
    }
  })
}

resource "aws_cloudwatch_event_target" "weather_task_failed" {
  count     = local.weather_enabled
  rule      = aws_cloudwatch_event_rule.weather_task_failed[0].name
  target_id = "weather-task-failed-sns"
  arn       = aws_sns_topic.alerts.arn
}

output "weather_task_definition" {
  description = "Task definition family for the daily ERA5 weather import."
  value       = local.weather_enabled == 0 ? "" : aws_ecs_task_definition.weather[0].family
}

output "weather_run_task_command" {
  description = "One-off run (today minus the lag). Append --overrides with a command to backfill a range or scope to --windfarm-ids."
  value = local.weather_enabled == 0 ? "" : join(" ", [
    "aws ecs run-task --cluster ${aws_ecs_cluster.this.name}",
    "--launch-type FARGATE",
    "--task-definition ${aws_ecs_task_definition.weather[0].family}",
    "--network-configuration 'awsvpcConfiguration={subnets=[${join(",", data.aws_subnets.default.ids)}],securityGroups=[${aws_security_group.service.id}],assignPublicIp=ENABLED}'",
    "--profile ${var.aws_profile} --region ${var.region}",
  ])
}
