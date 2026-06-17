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

variable "pipeline_daily_enabled" {
  description = "Enables the in-process APScheduler nightly pipeline. Keep false during Railway burn-in — true on both platforms means the pipeline runs twice a night. Flip to true at cutover."
  type        = bool
  default     = false
}

variable "pipeline_daily_hour" {
  description = "UTC hour the nightly pipeline runs. Default 3 matches Railway; offset (e.g. 5) during burn-in so AWS and Railway don't run concurrently against the shared RDS."
  type        = string
  default     = "3"
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
