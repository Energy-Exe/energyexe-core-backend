# EventBridge-driven scheduled imports.
#
# WHY THIS EXISTS: GitHub Actions `schedule:` is explicitly best-effort — runs
# are delayed or silently dropped under load. Measured on this repo over
# 2026-08-04..08-11, the hourly Taipower trigger fired on only 9-20 of 24 hours
# per day (15-60% dropped), with delays up to 3h17m.
#
# That is tolerable for the daily/monthly imports, which pull a *dated* window
# and catch up on the next run. It is NOT tolerable for Taipower: the API serves
# only the CURRENT snapshot (no historical endpoint), stamped on the hour, so a
# missed run is an hour of Taiwan generation lost permanently.
#
# WHY A LAMBDA SHIM AND NOT AN API DESTINATION: this was first built as
# EventBridge -> API Destination -> the trigger endpoint, and that fails.
# API Destinations enforce a hard 5-SECOND invocation timeout; the import runs
# synchronously (subprocess.run inside the request) and takes ~7s, so every
# delivery was recorded FAILED despite succeeding server-side, then retried —
# 9 imports an hour, a DLQ message and a false alarm every hour. Observed live:
# a rate(1 minute) test rule produced ~90 imports over 28 minutes of backoff.
# A Lambda can wait for the real response, so retries are driven by the actual
# outcome instead of by a timeout.
#
# WHY NOT AN IN-PROCESS APScheduler JOB (like app/cron/pipeline_daily.py):
# that scheduler uses APScheduler's default in-memory jobstore, so a fresh
# process computes next_run_time forward from now with no record of a missed
# fire — misfire_grace_time/coalesce only rescue a run when the process stayed
# alive. Any deploy, OOM or crash spanning :05 would lose that hour with no
# recovery, and the scheduler would share a failure domain with the service it
# triggers.
#
# The remaining GitHub crons (entsoe/elexon/eia/ecb + the price imports) stay
# where they are — they self-heal on the next run.

locals {
  scheduled_imports_enabled = var.api_domain == "" ? 0 : 1
}

# --- Lambda: calls the trigger endpoint and waits for the real result --------
data "archive_file" "trigger_import" {
  count       = local.scheduled_imports_enabled
  type        = "zip"
  source_file = "${path.module}/lambda/trigger_import/index.py"
  output_path = "${path.module}/lambda/trigger_import.zip"
}

resource "aws_lambda_function" "trigger_import" {
  count            = local.scheduled_imports_enabled
  function_name    = "${local.name}-trigger-import"
  description      = "POSTs /import-jobs/trigger/{job} and retries on the real outcome."
  role             = aws_iam_role.trigger_import[0].arn
  handler          = "index.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.trigger_import[0].output_path
  source_code_hash = data.archive_file.trigger_import[0].output_base64sha256

  # Must comfortably exceed the slowest import this fires plus its in-invocation
  # retries. Taipower is ~7s; the headroom is for the retry backoff.
  timeout     = 300
  memory_size = 128

  environment {
    variables = {
      API_URL = "https://${var.api_domain}"
    }
  }
}

resource "aws_cloudwatch_log_group" "trigger_import" {
  count             = local.scheduled_imports_enabled
  name              = "/aws/lambda/${local.name}-trigger-import"
  retention_in_days = 30
}

# Async invocations (which is how EventBridge calls Lambda) retry at most twice.
# On-failure sends the event to the DLQ, which alarms — so an hour that could
# not be imported at all is visible instead of silent.
resource "aws_lambda_function_event_invoke_config" "trigger_import" {
  count                        = local.scheduled_imports_enabled
  function_name                = aws_lambda_function.trigger_import[0].function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 1800 # 30 min — stay inside the hour

  destination_config {
    on_failure {
      destination = aws_sqs_queue.eventbridge_dlq[0].arn
    }
  }
}

# --- Schedule ---------------------------------------------------------------
#
# :05 keeps the historical slot and leaves the top of the hour clear of the
# 06/07/08/09 daily imports (the backend runs one task with --workers 1 and
# blocks on subprocess.run, so overlapping imports stall each other).
resource "aws_cloudwatch_event_rule" "taipower_hourly" {
  count               = local.scheduled_imports_enabled
  name                = "${local.name}-taipower-hourly"
  description         = "Hourly Taipower snapshot import (replaces the dropped GitHub cron)."
  schedule_expression = "cron(5 * * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "taipower_hourly" {
  count     = local.scheduled_imports_enabled
  rule      = aws_cloudwatch_event_rule.taipower_hourly[0].name
  target_id = "taipower-hourly-lambda"
  arn       = aws_lambda_function.trigger_import[0].arn
  input     = jsonencode({ job_name = "taipower-hourly" })
}

resource "aws_lambda_permission" "taipower_hourly" {
  count         = local.scheduled_imports_enabled
  statement_id  = "AllowExecutionFromEventBridgeTaipower"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_import[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.taipower_hourly[0].arn
}

# --- Dead letter queue ------------------------------------------------------
#
# An hour that exhausts the Lambda's in-invocation retries AND its two async
# retries lands here. Depth > 0 means a Taipower hour was lost for good — the
# failure the GitHub cron never reported, because a dropped schedule produces no
# run and therefore no red build.
resource "aws_sqs_queue" "eventbridge_dlq" {
  count                     = local.scheduled_imports_enabled
  name                      = "${local.name}-eventbridge-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_cloudwatch_metric_alarm" "eventbridge_dlq_not_empty" {
  count               = local.scheduled_imports_enabled
  alarm_name          = "${local.name}-eventbridge-dlq-not-empty"
  alarm_description   = "A scheduled import exhausted every retry — that hour's Taipower snapshot is lost."
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
resource "aws_iam_role" "trigger_import" {
  count = local.scheduled_imports_enabled
  name  = "${local.name}-trigger-import-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "trigger_import" {
  count = local.scheduled_imports_enabled
  name  = "logs-and-dlq"
  role  = aws_iam_role.trigger_import[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.trigger_import[0].arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.eventbridge_dlq[0].arn
      },
    ]
  })
}

output "trigger_import_lambda" {
  description = "Lambda that fires the scheduled import HTTP triggers."
  value       = local.scheduled_imports_enabled == 0 ? "" : aws_lambda_function.trigger_import[0].function_name
}

output "taipower_schedule_rule" {
  description = "EventBridge rule driving the hourly Taipower import."
  value       = local.scheduled_imports_enabled == 0 ? "" : aws_cloudwatch_event_rule.taipower_hourly[0].name
}
