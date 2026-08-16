# Infra-level alerting (the "is it up / is it 5xx-ing" safety net that an
# in-process error SDK structurally can't report). App-level exceptions go to
# GlitchTip; these CloudWatch alarms catch the task being down, the ALB
# returning 5xx, and the error tracker itself going dark — all delivered to
# email via SNS.
#
# ALB metrics (AWS/ApplicationELB) are emitted by default — no Container
# Insights required.

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

# Email subscriptions require a one-time confirmation click (AWS emails a link
# on apply). Empty alert_email = topic only, no subscription.
resource "aws_sns_topic_subscription" "alerts_email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# --- Backend: service down (no healthy targets) ---
#
# HealthyHostCount drops to 0 when the single task is gone/unhealthy. A normal
# deploy briefly drops it (~1-2 min) by design, so require 5 consecutive 1-min
# datapoints at 0 before firing — longer than any healthy deploy.
resource "aws_cloudwatch_metric_alarm" "backend_no_healthy_hosts" {
  alarm_name          = "${local.name}-no-healthy-hosts"
  alarm_description   = "Backend has no healthy targets for 5 min — the service is down."
  namespace           = "AWS/ApplicationELB"
  metric_name         = "HealthyHostCount"
  statistic           = "Minimum"
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 5
  treat_missing_data  = "breaching"

  dimensions = {
    LoadBalancer = aws_lb.this.arn_suffix
    TargetGroup  = aws_lb_target_group.api.arn_suffix
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- Backend: app returning 5xx to clients ---
resource "aws_cloudwatch_metric_alarm" "backend_target_5xx" {
  alarm_name          = "${local.name}-target-5xx"
  alarm_description   = "Backend returned 5xx responses (unhandled errors reaching clients)."
  namespace           = "AWS/ApplicationELB"
  metric_name         = "HTTPCode_Target_5XX_Count"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 5 # tune once the baseline is known
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching" # no requests = no 5xx = fine

  dimensions = {
    LoadBalancer = aws_lb.this.arn_suffix
    TargetGroup  = aws_lb_target_group.api.arn_suffix
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- Backend: ALB-level 5xx (502/503 — no healthy target, gateway errors) ---
resource "aws_cloudwatch_metric_alarm" "backend_elb_5xx" {
  alarm_name          = "${local.name}-elb-5xx"
  alarm_description   = "ALB returned 5xx for the backend (502/503 — can't reach a healthy target)."
  namespace           = "AWS/ApplicationELB"
  metric_name         = "HTTPCode_ELB_5XX_Count"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 3
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    LoadBalancer = aws_lb.this.arn_suffix
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- GlitchTip: the error tracker itself is down ---
#
# If GlitchTip goes dark you're blind to app errors, so alarm on it too. Only
# created once GlitchTip is serving (phase 2).
resource "aws_cloudwatch_metric_alarm" "glitchtip_no_healthy_hosts" {
  count               = local.glitchtip_serving
  alarm_name          = "${local.glitchtip_name}-no-healthy-hosts"
  alarm_description   = "GlitchTip has no healthy targets for 5 min — the error tracker is down."
  namespace           = "AWS/ApplicationELB"
  metric_name         = "HealthyHostCount"
  statistic           = "Minimum"
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 5
  treat_missing_data  = "breaching"

  dimensions = {
    LoadBalancer = aws_lb.this.arn_suffix
    TargetGroup  = aws_lb_target_group.glitchtip[0].arn_suffix
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- Backend: task memory ---
#
# Declared here to match what is already live. These four alarms were created
# out-of-band and existed in state with no configuration, so every untargeted
# `terraform apply` planned to DESTROY them — including this one, which is the
# only thing watching the known backend memory leak. Re-declared from the live
# definitions so `terraform plan` is clean and `-target` is no longer required.
resource "aws_cloudwatch_metric_alarm" "backend_memory_high" {
  alarm_name          = "${local.name}-memory-high"
  alarm_description   = "Backend task memory >90% for 15 min — OOM-kill / restart risk. Bump task_memory or decouple the pipeline."
  namespace           = "AWS/ECS"
  metric_name         = "MemoryUtilization"
  statistic           = "Average"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 90
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.this.name
    ServiceName = aws_ecs_service.api.name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- RDS ---
#
# The prod instance is console-managed, not Terraform-managed (see infra/README),
# so these reference it by identifier rather than by resource attribute.
locals {
  rds_instance_identifier = "energyexedb"
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu_high" {
  alarm_name          = "${local.name}-rds-cpu-high"
  alarm_description   = "energyexedb CPU >90% for 1h — pipeline overrun or a runaway query starving the live API."
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 90
  period              = 300
  evaluation_periods  = 12
  datapoints_to_alarm = 12
  treat_missing_data  = "notBreaching"

  dimensions    = { DBInstanceIdentifier = local.rds_instance_identifier }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "rds_low_memory" {
  alarm_name          = "${local.name}-rds-low-memory"
  alarm_description   = "energyexedb freeable memory <512 MB — risk of swapping / dropped connections."
  namespace           = "AWS/RDS"
  metric_name         = "FreeableMemory"
  statistic           = "Minimum"
  comparison_operator = "LessThanThreshold"
  threshold           = 536870912 # 512 MB
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  treat_missing_data  = "notBreaching"

  dimensions    = { DBInstanceIdentifier = local.rds_instance_identifier }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# treat_missing_data = "breaching" here (unlike the others): FreeStorageSpace is
# emitted continuously, so a gap means the instance stopped reporting — itself
# worth knowing about.
resource "aws_cloudwatch_metric_alarm" "rds_low_storage" {
  alarm_name          = "${local.name}-rds-low-storage"
  alarm_description   = "energyexedb free storage <25 GB — check growth / confirm storage autoscaling raises the ceiling."
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  statistic           = "Minimum"
  comparison_operator = "LessThanThreshold"
  threshold           = 26843545600 # 25 GB
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "breaching"

  dimensions    = { DBInstanceIdentifier = local.rds_instance_identifier }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

output "alerts_sns_topic_arn" {
  description = "SNS topic CloudWatch alarms publish to."
  value       = aws_sns_topic.alerts.arn
}
