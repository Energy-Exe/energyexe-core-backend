# Nightly performance pipeline — EventBridge -> one-off ECS Fargate task.
#
# WHY THIS EXISTS: this job used to run on an in-process APScheduler inside the
# API container (app/cron/pipeline_daily.py). Two problems with that:
#
#  1. APScheduler's default jobstore is IN-MEMORY. A fresh process computes
#     next_run_time forward from now with no record of a missed fire, so any
#     deploy, OOM or crash spanning 03:00 lost that night with no recovery.
#     misfire_grace_time/coalesce only rescue a run if the process stayed alive.
#  2. It ran for ~2h50m (measured 2026-08-16: 8010s batch + 2201s detection)
#     inside the single uvicorn worker serving live API traffic. That is why
#     `--workers 1` and `desired_count = 1` are pinned, which is in turn why
#     every deploy costs 1-2 minutes of downtime.
#
# A run-to-completion task fixes both: the schedule lives outside the thing it
# triggers, and the work no longer competes with request handling.
#
# It is a SEPARATE task-definition family rather than a command override on the
# API task def, so the API can be sized independently and a RunTask can't
# silently change behaviour when the API task def is revised.
#
# NOTE: this does not reduce DATABASE load — the same queries hit the same RDS
# instance (observed: RDS CPU 18-34% avg, peaking 64% during detection). What it
# removes is event-loop contention and the lost-night failure mode.

locals {
  # Same gate the imports use: no API domain means nothing to schedule against.
  pipeline_enabled = var.api_domain == "" ? 0 : 1
}

resource "aws_cloudwatch_log_group" "pipeline" {
  count             = local.pipeline_enabled
  name              = "/ecs/${local.name}-pipeline"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "pipeline" {
  count                    = local.pipeline_enabled
  family                   = "${local.name}-pipeline"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.pipeline_task_cpu
  memory                   = var.pipeline_task_memory

  # Both roles are reused as-is. Their policies are scoped to secrets and S3
  # buckets, not to task identity, so nothing needs widening.
  execution_role_arn = aws_iam_role.execution.arn
  task_role_arn      = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([{
    name      = "pipeline"
    image     = "${aws_ecr_repository.this.repository_url}:${var.image_tag}"
    essential = true

    # The Dockerfile sets CMD but no ENTRYPOINT, so this replaces it outright.
    # `COPY . .` already ships scripts/, so no image change was needed.
    #
    # Deliberately NOT `alembic upgrade head && ...`: the API service owns
    # migrations, and a concurrent alembic here would race a deploy.
    command = ["python", "scripts/jobs/run_pipeline_daily.py"]

    # No portMappings — nothing connects to this task.

    environment = [
      { name = "LOG_LEVEL", value = "INFO" },
      { name = "SENTRY_ENVIRONMENT", value = "production" },
      # Without this, Python block-buffers stdout when not on a TTY and
      # CloudWatch gets nothing until the container exits — which is useless for
      # a 3-hour job, and worse than useless if it dies.
      { name = "PYTHONUNBUFFERED", value = "1" },
      # These no longer schedule anything. They tell the GlitchTip cron monitor
      # when to EXPECT a check-in, so they must match the rule below or GlitchTip
      # alerts on runs that happened perfectly well.
      { name = "PIPELINE_DAILY_HOUR", value = tostring(var.pipeline_daily_hour) },
      { name = "PIPELINE_DAILY_MINUTE", value = "0" },
      # One process, no API traffic to serve: it does not need the API's pool.
      { name = "DB_POOL_SIZE", value = "5" },
      { name = "DB_MAX_OVERFLOW", value = "5" },
      { name = "AWS_DEFAULT_REGION", value = var.region },
    ]

    # Only what the job actually reads. The pipeline/detection/peer-aggregate
    # chain makes zero HTTP, LLM or Redis calls, so the other secrets the API
    # gets are all unnecessary here.
    #
    # SENTRY_DSN is not optional in practice: without it cron_checkin() and
    # capture_exception() are silent no-ops and a failed night reports nothing.
    secrets = concat(
      [{ name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn }],
      var.backend_sentry_dsn_enabled ? [
        { name = "SENTRY_DSN", valueFrom = aws_secretsmanager_secret.backend_sentry_dsn.arn }
      ] : []
    )

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.pipeline[0].name
        awslogs-region        = var.region
        awslogs-stream-prefix = "pipeline"
      }
    }
  }])
}

# --- Schedule ---------------------------------------------------------------
#
# 03:00 UTC, unchanged from the APScheduler default — after the weather and
# generation imports settle (~02:30) and finishing before the 06:00 import.
resource "aws_cloudwatch_event_rule" "pipeline_daily" {
  count               = local.pipeline_enabled
  name                = "${local.name}-pipeline-daily"
  description         = "Nightly performance pipeline + opportunity detection (replaces the in-process APScheduler job)."
  schedule_expression = "cron(0 ${var.pipeline_daily_hour} * * ? *)"
  state               = var.pipeline_schedule_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "pipeline_daily" {
  count     = local.pipeline_enabled
  rule      = aws_cloudwatch_event_rule.pipeline_daily[0].name
  target_id = "pipeline-daily-task"
  arn       = aws_ecs_cluster.this.arn
  role_arn  = aws_iam_role.pipeline_events[0].arn

  # NOTE: `ecs_target`, not `ecs_parameters` — the latter belongs to
  # aws_scheduler_schedule (EventBridge Scheduler), which this root does not use.
  ecs_target {
    task_definition_arn = aws_ecs_task_definition.pipeline[0].arn
    task_count          = 1
    launch_type         = "FARGATE"

    network_configuration {
      subnets         = data.aws_subnets.default.ids
      security_groups = [aws_security_group.service.id]
      # Required: the default VPC has no NAT gateway, so a task without a public
      # IP cannot reach ECR to pull its own image. Reusing the service SG also
      # inherits RDS access via aws_vpc_security_group_ingress_rule.rds_from_service,
      # with zero changes to the console-managed RDS security group.
      assign_public_ip = true
    }
  }

  # This retries the RunTask *invocation*, not the job. Correct semantics: a
  # transient ECS API failure gets another go, but a pipeline that ran and failed
  # is NOT silently re-run — a second 3-hour pass would collide with the 06:00
  # imports. Failures alarm instead (see below).
  retry_policy {
    maximum_retry_attempts       = 3
    maximum_event_age_in_seconds = 3600
  }
}

# --- IAM: let EventBridge start the task ------------------------------------
resource "aws_iam_role" "pipeline_events" {
  count = local.pipeline_enabled
  name  = "${local.name}-pipeline-events"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "pipeline_events" {
  count = local.pipeline_enabled
  name  = "run-task"
  role  = aws_iam_role.pipeline_events[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        # arn_without_revision + :* so the policy survives task-def revisions.
        Resource = "${aws_ecs_task_definition.pipeline[0].arn_without_revision}:*"
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
#
# The job's exit code is the signal (0 ok / 1 batch failed / 2 detection failed),
# and GlitchTip's cron monitor catches a night that never ran at all. This rule
# covers the third case: the task died in a way the process could not report —
# OOM kill, image pull failure, Fargate capacity error.
#
# Deliberately NOT filtered on `containers.exitCode` alone. That requires a
# container that actually started, so a pull failure or a pre-start OOM produces
# a STOPPED task with no exitCode at all and would never match. Matching on
# stopCode instead catches both: a clean run sets EssentialContainerExited with
# exit 0, which the exitCode filter excludes.
resource "aws_cloudwatch_event_rule" "pipeline_task_failed" {
  count       = local.pipeline_enabled
  name        = "${local.name}-pipeline-task-failed"
  description = "Nightly pipeline task stopped abnormally or exited non-zero."

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      lastStatus        = ["STOPPED"]
      clusterArn        = [aws_ecs_cluster.this.arn]
      taskDefinitionArn = [{ prefix = "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task-definition/${local.name}-pipeline" }]
      "$or" = [
        { stopCode = [{ "anything-but" = ["EssentialContainerExited"] }] },
        { containers = { exitCode = [{ "anything-but" = [0] }] } },
      ]
    }
  })
}

resource "aws_cloudwatch_event_target" "pipeline_task_failed" {
  count     = local.pipeline_enabled
  rule      = aws_cloudwatch_event_rule.pipeline_task_failed[0].name
  target_id = "pipeline-task-failed-sns"
  arn       = aws_sns_topic.alerts.arn
}

# EventBridge -> SNS needs an explicit topic policy. The topic was on the AWS
# default policy, which this REPLACES — so the default owner statement is
# reproduced verbatim below. Dropping it would silently break delivery of every
# existing CloudWatch alarm.
resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn

  policy = jsonencode({
    Version = "2008-10-17"
    Id      = "energyexe-core-backend-alerts-policy"
    Statement = [
      {
        Sid       = "__default_statement_ID"
        Effect    = "Allow"
        Principal = { AWS = "*" }
        Action = [
          "SNS:GetTopicAttributes",
          "SNS:SetTopicAttributes",
          "SNS:AddPermission",
          "SNS:RemovePermission",
          "SNS:DeleteTopic",
          "SNS:Subscribe",
          "SNS:ListSubscriptionsByTopic",
          "SNS:Publish",
        ]
        Resource  = aws_sns_topic.alerts.arn
        Condition = { StringEquals = { "AWS:SourceOwner" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid       = "AllowEventBridgePublish"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.alerts.arn
      },
    ]
  })
}

output "pipeline_task_definition" {
  description = "Task definition family for the nightly performance pipeline."
  value       = local.pipeline_enabled == 0 ? "" : aws_ecs_task_definition.pipeline[0].family
}

output "pipeline_run_task_command" {
  description = "Ready-to-run smoke test. Append --windfarm-ids to scope it."
  value = local.pipeline_enabled == 0 ? "" : join(" ", [
    "aws ecs run-task --cluster ${aws_ecs_cluster.this.name}",
    "--launch-type FARGATE",
    "--task-definition ${aws_ecs_task_definition.pipeline[0].family}",
    "--network-configuration 'awsvpcConfiguration={subnets=[${join(",", data.aws_subnets.default.ids)}],securityGroups=[${aws_security_group.service.id}],assignPublicIp=ENABLED}'",
    "--profile ${var.aws_profile} --region ${var.region}",
  ])
}
