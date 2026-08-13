# EventBridge-driven scheduled imports.
#
# WHY THIS EXISTS: GitHub Actions `schedule:` is explicitly best-effort — runs
# are delayed or silently dropped under load. Measured on this repo over
# 2026-08-04..08-11, the hourly Taipower trigger fired on only 9-20 of 24 hours
# per day (15-60% dropped), with delays up to 3h17m.
#
# That is tolerable for the daily/monthly imports, which pull a *dated* window
# and simply catch up the next day. It is NOT tolerable for Taipower: the API
# serves only the CURRENT snapshot (no historical endpoint), stamped on the
# hour, so a missed run is an hour of Taiwan generation lost permanently.
#
# WHY NOT AN IN-PROCESS APScheduler JOB (like app/cron/pipeline_daily.py):
# that scheduler uses APScheduler's default in-memory jobstore, so a fresh
# process computes next_run_time forward from now and has no record of a missed
# fire — misfire_grace_time/coalesce only rescue a run when the process stayed
# alive. Any deploy, OOM or crash spanning :05 would lose that hour with no
# recovery, and the scheduler would share a failure domain with the very
# service it triggers. EventBridge is external to the app and retries.
#
# The remaining GitHub crons (entsoe/elexon/eia/ecb, and the price imports) stay
# where they are — they self-heal on the next run.

locals {
  taipower_trigger_url = var.api_domain == "" ? "" : "https://${var.api_domain}/api/v1/import-jobs/trigger/taipower-hourly"
  taipower_enabled     = var.api_domain == "" ? 0 : 1
}

# --- Connection -------------------------------------------------------------
#
# API Destinations REQUIRE a Connection with an authorization type, even when
# the target needs no auth. `/import-jobs/trigger/{job}` is deliberately public
# (see the endpoint docstring), so this sends a descriptive header rather than a
# credential — there is no secret here, and none should be added to this file.
# If that endpoint ever gains auth, this is the seam: swap the value for a
# Secrets Manager lookup.
resource "aws_cloudwatch_event_connection" "api_trigger" {
  count              = local.taipower_enabled
  name               = "${local.name}-api-trigger"
  description        = "Calls the public /import-jobs/trigger endpoints. Header is an identifier, not a credential."
  authorization_type = "API_KEY"

  auth_parameters {
    api_key {
      key   = "X-Triggered-By"
      value = "eventbridge"
    }
  }
}

# --- Destination ------------------------------------------------------------
resource "aws_cloudwatch_event_api_destination" "taipower" {
  count                            = local.taipower_enabled
  name                             = "${local.name}-taipower-hourly"
  description                      = "POST the Taipower hourly snapshot import."
  invocation_endpoint              = local.taipower_trigger_url
  http_method                      = "POST"
  connection_arn                   = aws_cloudwatch_event_connection.api_trigger[0].arn
  invocation_rate_limit_per_second = 1
}

# --- Schedule ---------------------------------------------------------------
#
# :05 keeps the historical slot, and leaves the top of the hour clear of the
# 06/07/08/09 daily imports (the backend runs one task with --workers 1 and
# blocks on subprocess.run, so overlapping imports stall each other).
# ⛔ DISABLED 2026-08-13, and it must stay disabled until the blocker below is
# fixed. EventBridge API Destinations enforce a hard 5-SECOND invocation
# timeout. The Taipower import answers in ~7s (execute_job runs subprocess.run
# synchronously inside the request), so EventBridge records every delivery as
# FAILED even though the import succeeds server-side — and retries it. One
# scheduled fire becomes 9 imports, a DLQ message, and a false alarm, every
# hour. Observed live: a rate(1 minute) test rule produced ~90 imports over 28
# minutes of backoff (no data damage — the upserts are idempotent — but the
# single worker was blocked ~37% of that window).
#
# To enable: make /import-jobs/trigger/{job} answer inside 5s (return 202 and
# run the import in the background — which would also lift the 150s
# blocked-worker health-check ceiling), or front this with a Lambda that calls
# the endpoint and retries on the real outcome. Then set state = "ENABLED".
#
# Also note: `aws events put-targets` without an explicit RetryPolicy defaults
# to 185 attempts over 24h, and DELETING a rule does not cancel already-queued
# retries — deauthorize the connection to stop those.
resource "aws_cloudwatch_event_rule" "taipower_hourly" {
  count               = local.taipower_enabled
  name                = "${local.name}-taipower-hourly"
  description         = "Hourly Taipower snapshot import. DISABLED — see the comment above before enabling."
  schedule_expression = "cron(5 * * * ? *)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "taipower_hourly" {
  count     = local.taipower_enabled
  rule      = aws_cloudwatch_event_rule.taipower_hourly[0].name
  target_id = "taipower-hourly-api"
  arn       = aws_cloudwatch_event_api_destination.taipower[0].arn
  role_arn  = aws_iam_role.eventbridge_invoke_api[0].arn

  # The import takes ~7s, but a deploy takes the single task down for ~1-2 min.
  # Retry across that window; stop before the next hour's fire, because a retry
  # that lands after :00 would just re-fetch the *next* hour's snapshot, which
  # the next scheduled run does anyway.
  retry_policy {
    maximum_event_age_in_seconds = 3000 # 50 min
    maximum_retry_attempts       = 8
  }

  dead_letter_config {
    arn = aws_sqs_queue.eventbridge_dlq[0].arn
  }
}

# --- Dead letter queue ------------------------------------------------------
#
# An hour that exhausts every retry lands here. Depth > 0 is the signal that a
# Taipower hour was lost for good — the failure the GitHub cron never reported,
# because a dropped schedule produces no run and therefore no red build.
resource "aws_sqs_queue" "eventbridge_dlq" {
  count                     = local.taipower_enabled
  name                      = "${local.name}-eventbridge-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue_policy" "eventbridge_dlq" {
  count     = local.taipower_enabled
  queue_url = aws_sqs_queue.eventbridge_dlq[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.eventbridge_dlq[0].arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.taipower_hourly[0].arn }
      }
    }]
  })
}

resource "aws_cloudwatch_metric_alarm" "eventbridge_dlq_not_empty" {
  count               = local.taipower_enabled
  alarm_name          = "${local.name}-eventbridge-dlq-not-empty"
  alarm_description   = "A scheduled import exhausted its retries — that hour's Taipower snapshot is lost."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  dimensions    = { QueueName = aws_sqs_queue.eventbridge_dlq[0].name }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- IAM --------------------------------------------------------------------
resource "aws_iam_role" "eventbridge_invoke_api" {
  count = local.taipower_enabled
  name  = "${local.name}-eventbridge-invoke-api"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_invoke_api" {
  count = local.taipower_enabled
  name  = "invoke-api-destination"
  role  = aws_iam_role.eventbridge_invoke_api[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "events:InvokeApiDestination"
      Resource = aws_cloudwatch_event_api_destination.taipower[0].arn
    }]
  })
}

output "taipower_schedule_rule" {
  description = "EventBridge rule driving the hourly Taipower import."
  value       = local.taipower_enabled == 0 ? "" : aws_cloudwatch_event_rule.taipower_hourly[0].name
}
