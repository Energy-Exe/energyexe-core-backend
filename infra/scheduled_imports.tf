# EventBridge-driven scheduled imports.
#
# WHY THIS EXISTS: GitHub Actions `schedule:` is explicitly best-effort — runs
# are delayed or silently dropped under load. Measured on this repo over
# 2026-08-04..08-11, the hourly Taipower trigger fired on only 9-20 of 24 hours
# per day (15-60% dropped), with delays up to 3h17m.
#
# Taipower moved here first because it is the one that cannot self-heal: the API
# serves only the CURRENT snapshot (no historical endpoint), stamped on the hour,
# so a missed run is an hour of Taiwan generation lost permanently. The dated
# imports do catch up on the next run, but they were still landing 21-80 MINUTES
# late on every single run (measured 2026-08-14..08-16), and the spacing between
# them is not cosmetic — the backend runs one task with one worker and
# execute_job blocks on subprocess.run, so separation is what stops two imports
# stalling each other. Under GitHub that separation was luck; here it is
# guaranteed. So all of them now live in this file and nothing schedules work
# from outside AWS.
#
# WHY THE DAILY BATCH RUNS AT ~MIDNIGHT NORWAY (2026-09-05): while an import
# runs, the single worker's event loop is frozen — /health times out, the ALB
# marks the target unhealthy after 150s and ECS restarts the task. At the old
# 06:00/08:00 UTC slots (08:00/10:00 Oslo) that took the API down for 1-3 min in
# the middle of the working day, twice a day, and dropped every live Brain-agent
# stream. The batch now runs 22:10-22:55 UTC = 00:10-00:55 Oslo in summer and
# 23:10-23:55 in winter (EventBridge cron is UTC-only; the 1h DST drift is
# irrelevant at night). Every job's date window is derived from the UTC date
# (import_jobs.py), so staying inside the same UTC day imports exactly the same
# data as before. The batch sits between two hourly Taipower runs (:05), and the
# nightly ECS tasks (weather 01:30, pipeline 03:00) run after it, unchanged.
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
locals {
  scheduled_imports_enabled = var.api_domain == "" ? 0 : 1

  # Every scheduled import, keyed by the job_name the trigger endpoint expects —
  # the key is passed straight through to the Lambda as {"job_name": <key>}.
  # Adding a job here is the whole change: rule, target and permission all fan
  # out from this map.
  #
  # Times are mirrored in app/services/import_job_service.py::IMPORT_SCHEDULES,
  # which drives the admin /import-jobs "next run" column;
  # tests/test_import_schedules.py parses this file and fails when they drift.
  #
  # EventBridge cron is 6 fields (min hour day-of-month month day-of-week year)
  # and exactly one of day-of-month / day-of-week must be "?".
  import_schedules = local.scheduled_imports_enabled == 0 ? {} : {
    "taipower-hourly" = {
      expression  = "cron(5 * * * ? *)"
      description = "Hourly Taipower snapshot — current-snapshot API, a missed hour is unrecoverable."
    }
    # Night batch (~midnight Oslo, see header): 22:10-22:55 UTC, inside one UTC
    # day, spaced 10 min apart (ENTSOE runs take up to ~4 min while execute_job
    # still blocks the worker), and clear of the :05 Taipower run either side.
    "entsoe-daily" = {
      expression  = "cron(10 22 * * ? *)"
      description = "Daily ENTSOE generation import (22:10 UTC ≈ midnight Oslo)."
    }
    "elexon-daily" = {
      expression  = "cron(20 22 * * ? *)"
      description = "Daily Elexon generation import (22:20 UTC, after the ENTSOE generation run)."
    }
    "entsoe-prices-daily" = {
      expression  = "cron(30 22 * * ? *)"
      description = "Daily ENTSOE day-ahead price import (22:30 UTC, after the generation runs)."
    }
    "elexon-prices-daily" = {
      expression  = "cron(40 22 * * ? *)"
      description = "Daily Elexon market index price import (22:40 UTC, after the ENTSOE price run)."
    }
    "ecb-rates-daily" = {
      expression  = "cron(50 22 ? * MON-FRI *)"
      description = "ECB exchange rates on weekdays (22:50 UTC — same-day rate, published ~16:00 CET)."
    }
    "eia-monthly" = {
      expression  = "cron(55 22 1 * ? *)"
      description = "Monthly EIA import (22:55 UTC on the 1st, tail of the night batch)."
    }
  }
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

  # The endpoint answers when the import has finished: 40-110s Taipower,
  # up to ~4 min for the ENTSOE jobs. One attempt may wait READ_TIMEOUT_SECONDS
  # (kept under the ALB's 300s idle timeout); the handler sizes any further
  # attempt to the time left, so the function timeout is never the thing that
  # cuts an answer off.
  timeout     = 300
  memory_size = 128

  environment {
    variables = {
      API_URL              = "https://${var.api_domain}"
      READ_TIMEOUT_SECONDS = "270"
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

# --- Schedules --------------------------------------------------------------
#
# Taipower runs at :05 every hour; the daily batch is placed at :10-:55 of the
# 22:00 UTC hour so nothing overlaps it, for the single-worker reason above.
resource "aws_cloudwatch_event_rule" "import" {
  for_each            = local.import_schedules
  name                = "${local.name}-${each.key}"
  description         = each.value.description
  schedule_expression = each.value.expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "import" {
  for_each  = local.import_schedules
  rule      = aws_cloudwatch_event_rule.import[each.key].name
  target_id = "${each.key}-lambda"
  arn       = aws_lambda_function.trigger_import[0].arn
  input     = jsonencode({ job_name = each.key })
}

resource "aws_lambda_permission" "import" {
  for_each      = local.import_schedules
  statement_id  = "AllowExecutionFromEventBridge-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_import[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.import[each.key].arn
}

# Taipower was these resources before the map existed. Without these `moved`
# blocks Terraform would destroy and recreate the live hourly rule.
moved {
  from = aws_cloudwatch_event_rule.taipower_hourly[0]
  to   = aws_cloudwatch_event_rule.import["taipower-hourly"]
}

moved {
  from = aws_cloudwatch_event_target.taipower_hourly[0]
  to   = aws_cloudwatch_event_target.import["taipower-hourly"]
}

moved {
  from = aws_lambda_permission.taipower_hourly[0]
  to   = aws_lambda_permission.import["taipower-hourly"]
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

output "import_schedule_rules" {
  description = "EventBridge rules driving the scheduled imports, keyed by job name."
  value       = { for k, r in aws_cloudwatch_event_rule.import : k => r.schedule_expression }
}
