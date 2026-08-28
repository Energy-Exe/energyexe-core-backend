variable "region" {
  description = "AWS region (RDS energyexedb lives here)"
  type        = string
  default     = "eu-north-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "energyexe"
}

variable "rds_security_group_id" {
  description = "Existing security group of the energyexedb RDS instance. Terraform only ADDS an ingress rule to it; the SG itself stays unmanaged."
  type        = string
  default     = "sg-08ce9488ba4aa1fde"
}

variable "image_tag" {
  description = "Image tag the task definition points at. CI pushes :latest plus a git-SHA tag."
  type        = string
  default     = "latest"
}

variable "task_cpu" {
  description = "Fargate task CPU units (1024 = 1 vCPU)"
  type        = number
  default     = 1024
}

variable "task_memory" {
  description = "Fargate task memory in MiB. Sized for the in-container import jobs (pandas), not just the API."
  type        = number
  default     = 4096
}

variable "cpu_architecture" {
  description = "X86_64 builds cleanly on standard GitHub Actions runners; ARM64 is ~20% cheaper and native on Apple Silicon but needs an arm64 build pipeline."
  type        = string
  default     = "X86_64"
}

variable "certificate_arn" {
  description = "ACM certificate ARN for HTTPS. Empty = HTTP-only on the ALB DNS name (fine for burn-in, add a cert before pointing real frontends here)."
  type        = string
  default     = ""
}

variable "s3_bucket_name" {
  description = "Existing S3 bucket for brain-agent image persistence (task role gets scoped access)"
  type        = string
  default     = "energyexe-agent-images"
}

variable "scada_data_bucket" {
  description = "SCADA pipeline data lake bucket; task role gets READ-ONLY access to its silver/ prefix (brain-agent 10-min queries)"
  type        = string
  default     = "energyexe-scada-data"
}

variable "pipeline_daily_hour" {
  description = "UTC hour the nightly pipeline task runs. Drives both the EventBridge rule and the PIPELINE_DAILY_HOUR the container reports to GlitchTip — keep them derived from this one value so they cannot drift."
  type        = string
  default     = "3"
}

variable "pipeline_schedule_enabled" {
  description = "Enables the nightly pipeline EventBridge rule. Default false so the task definition can be created and smoke-tested by hand before anything fires on a schedule."
  type        = bool
  default     = false
}

variable "pipeline_task_cpu" {
  description = "Fargate CPU units for the nightly pipeline task. Mirrors the API's 2048: measured peak was ~47% of 2 vCPU, but tfvars records 97% CPU at 1 vCPU, so don't shrink without evidence."
  type        = number
  default     = 2048
}

variable "pipeline_task_memory" {
  description = "Fargate memory (MiB) for the nightly pipeline task. An OOM here loses the whole night, and the task only runs ~3h/day, so headroom is cheap."
  type        = number
  default     = 8192
}

variable "weather_daily_hour" {
  description = "UTC hour the daily ERA5 weather task runs (EPR-121). Drives the EventBridge rule and the WEATHER_DAILY_HOUR the container reports to GlitchTip. Must finish before pipeline_daily_hour."
  type        = string
  default     = "1"
}

variable "weather_daily_minute" {
  description = "UTC minute of the daily ERA5 weather task."
  type        = string
  default     = "30"
}

variable "weather_schedule_enabled" {
  description = "Enables the daily weather EventBridge rule. Default false so the task definition can be smoke-tested by hand (and the 2026 backfill run) before anything fires on a schedule."
  type        = bool
  default     = false
}

variable "weather_lag_days" {
  description = "The scheduled run imports today minus this many days — ERA5T is published ~5-6 days behind real time (CDS catalogue end lagged 6 days on 2026-08-28)."
  type        = number
  default     = 6
}

variable "weather_task_cpu" {
  description = "Fargate CPU units for the weather task. One CDS GRIB per day; decoding + interpolating ~1,600 farms is a few CPU-minutes."
  type        = number
  default     = 1024
}

variable "weather_task_memory" {
  description = "Fargate memory (MiB) for the weather task. xarray holds the whole-fleet bbox (~4M cells x 24h x 6 variables) in memory while interpolating — 8 GB is headroom, not a measurement."
  type        = number
  default     = 8192
}

variable "cors_origins" {
  description = "Optional BACKEND_CORS_ORIGINS override as a JSON array string, e.g. '[\"https://app.energyexe.com\"]'. Empty = use the app's built-in default list (already includes prod + dev origins)."
  type        = string
  default     = ""
}

# --- GlitchTip (self-hosted error tracker) — see glitchtip.tf ---

variable "glitchtip_domain" {
  description = "Custom domain for GlitchTip, e.g. errors.energyexe.com. Empty = GlitchTip disabled. Setting this (phase 1) creates the ACM cert + secret containers + SG + IAM."
  type        = string
  default     = ""
}

variable "glitchtip_certificate_arn" {
  description = "ACM cert ARN for glitchtip_domain. Empty until the cert is ISSUED. Setting this (phase 2) creates the target group, listener wiring, task definition, and service. Requires var.certificate_arn (the API HTTPS listener) to exist."
  type        = string
  default     = ""
}

variable "glitchtip_image_tag" {
  description = "Tag of the GlitchTip image mirrored into the private ECR repo (see glitchtip.tf). PIN a real release tag before production (https://gitlab.com/glitchtip/glitchtip/-/releases); 'latest' is only for first bring-up."
  type        = string
  default     = "latest"
}

variable "glitchtip_task_cpu" {
  description = "Fargate CPU units for the GlitchTip task (web+worker+redis). 512 = 0.5 vCPU."
  type        = number
  default     = 512
}

variable "glitchtip_task_memory" {
  description = "Fargate memory (MiB) for the GlitchTip task."
  type        = number
  default     = 2048
}

variable "backend_sentry_dsn_enabled" {
  description = "When true, the backend task reads SENTRY_DSN from Secrets Manager and reports errors to GlitchTip. Keep false until GlitchTip is up, a project exists, and the energyexe/core-backend/sentry-dsn secret is populated — otherwise the running backend can't start (empty secret)."
  type        = bool
  default     = false
}

variable "alert_email" {
  description = "Email address for CloudWatch alarm notifications (SNS). Empty = create the SNS topic but no subscription. The subscription needs a one-time confirmation click. See monitoring.tf."
  type        = string
  default     = ""
}

variable "bastion_security_group_id" {
  description = "SG of the SSM break-glass bastion allowed Postgres to the prod RDS (energyexe-scada-bastion; instance stays STOPPED between uses). Empty disables the rule. Prod RDS is PRIVATE since 2026-07-15 — this tunnel is the only operator path."
  type        = string
  default     = ""
}

# --- Production frontends (S3 + CloudFront, replacing Vercel — frontend.tf) ---

variable "admin_ui_domain" {
  description = "Production hostname of the admin-ui (CloudFront alias once frontend_certificate_arn is set)."
  type        = string
  default     = "dashboard.energyexe.com"
}

variable "client_ui_domain" {
  description = "Production hostname of the client-ui (CloudFront alias once frontend_certificate_arn is set)."
  type        = string
  default     = "app.energyexe.com"
}

variable "frontend_certificate_arn" {
  description = "us-east-1 ACM cert ARN for *.energyexe.com. Empty until DNS-validated (phase 1); setting it (phase 2) attaches the alias + cert to BOTH prod frontend CloudFront dists. Wildcard on purpose — per-hostname certs cannot issue while app./dashboard. still CNAME to Vercel (CAA), see frontend.tf."
  type        = string
  default     = ""
}

variable "github_admin_ui_repo" {
  description = "owner/repo of the admin-ui; its master branch may deploy to the prod admin bucket + invalidate its CloudFront dist."
  type        = string
  default     = "faisal-energyexe/energyexe-admin-ui"
}

variable "github_client_ui_repo" {
  description = "owner/repo of the client-ui; its main branch may deploy to the prod client bucket + invalidate its CloudFront dist."
  type        = string
  default     = "faisal-energyexe/energyexe-client-ui"
}
