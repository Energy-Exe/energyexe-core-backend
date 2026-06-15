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
  description = "Enables the in-process APScheduler nightly pipeline (03:00 UTC). Keep false during Railway burn-in — true on both platforms means the pipeline runs twice a night. Flip to true at cutover."
  type        = bool
  default     = false
}

variable "cors_origins" {
  description = "Optional BACKEND_CORS_ORIGINS override as a JSON array string, e.g. '[\"https://app.energyexe.com\"]'. Empty = use the app's built-in default list (already includes prod + dev origins)."
  type        = string
  default     = ""
}
