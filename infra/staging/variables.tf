variable "region" {
  description = "AWS region (shared with prod + RDS)"
  type        = string
  default     = "eu-north-1"
}

variable "aws_profile" {
  description = "AWS CLI profile"
  type        = string
  default     = "energyexe"
}

# --- Shared prod infra consumed read-only (see data.tf) ---

variable "prod_alb_name" {
  description = "Name of the existing prod ALB whose :443 listener staging attaches a host rule to."
  type        = string
  default     = "energyexe-core-backend"
}

variable "shared_cluster_name" {
  description = "Existing ECS cluster shared with prod."
  type        = string
  default     = "energyexe"
}

# --- Staging RDS ---

variable "rds_snapshot_identifier" {
  description = "DB snapshot of energyexedb to restore staging from. Create one first: aws rds create-db-snapshot --db-instance-identifier energyexedb --db-snapshot-identifier energyexedb-staging-seed --profile energyexe. Refreshing staging data = new snapshot + change this (forces a new instance)."
  type        = string
}

variable "rds_instance_class" {
  description = "Staging DB instance class. Snapshot storage stays at prod's ~200GB floor regardless."
  type        = string
  default     = "db.t4g.micro"
}

# --- Staging backend task ---

variable "task_cpu" {
  description = "Fargate CPU units for the staging API task (the nightly pipeline is OFF, so it only serves the API)."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Fargate memory (MiB) for the staging API task."
  type        = number
  default     = 1024
}

variable "cpu_architecture" {
  description = "Match the image build arch. Fargate + GitHub runners are X86_64."
  type        = string
  default     = "X86_64"
}

variable "image_tag" {
  description = "Image tag in the staging ECR repo the task runs. CI moves the `staging` tag on each staging-branch push."
  type        = string
  default     = "staging"
}

variable "s3_bucket_name" {
  description = "S3 bucket for brain-agent images. Reuses the prod bucket (task role gets scoped access)."
  type        = string
  default     = "energyexe-agent-images"
}

variable "backend_sentry_dsn_enabled" {
  description = "When true, wire SENTRY_DSN from the staging secret into the task. Keep false until the energyexe/core-backend-staging/sentry-dsn secret is populated, else the task can't start on an empty secret."
  type        = bool
  default     = false
}

# --- Domains + certs (two-phase, like prod, because DNS is manual at hyp.net) ---

variable "staging_api_domain" {
  description = "Hostname for the staging API on the shared ALB."
  type        = string
  default     = "staging-api.energyexe.com"
}

variable "staging_api_certificate_arn" {
  description = "ACM cert ARN (eu-north-1) for staging_api_domain. Empty until DNS-validated; setting it (phase 2) wires the ALB host rule + SNI cert. Phase 1 creates the cert + outputs the validation record."
  type        = string
  default     = ""
}

# --- Staging frontends on AWS (S3 + CloudFront) ---
# Direct-child hostnames of energyexe.com so ACM's CAA tree-walk never touches
# the Vercel-CNAME'd dashboard.* / app.* labels (whose CAA excludes Amazon). See
# frontend.tf for the full rationale. Two-phase like the API: phase 1 issues the
# us-east-1 cert (empty arn), phase 2 sets the arn to attach the CloudFront alias.

variable "admin_staging_domain" {
  description = "Hostname for the staging admin-ui (CloudFront). Direct child of energyexe.com."
  type        = string
  default     = "staging-dashboard.energyexe.com"
}

variable "client_staging_domain" {
  description = "Hostname for the staging client-ui (CloudFront). Direct child of energyexe.com."
  type        = string
  default     = "staging-app.energyexe.com"
}

variable "admin_certificate_arn" {
  description = "ACM cert ARN (us-east-1) for admin_staging_domain. Empty until DNS-validated; setting it (phase 2) attaches the alias + cert to the admin CloudFront dist."
  type        = string
  default     = ""
}

variable "client_certificate_arn" {
  description = "ACM cert ARN (us-east-1) for client_staging_domain. Empty until DNS-validated; setting it (phase 2) attaches the alias + cert to the client CloudFront dist."
  type        = string
  default     = ""
}

variable "github_admin_ui_repo" {
  description = "owner/repo of the admin-ui; its `staging` branch may deploy to the staging admin bucket + invalidate its CloudFront dist."
  type        = string
  default     = "faisal-energyexe/energyexe-admin-ui"
}

variable "github_client_ui_repo" {
  description = "owner/repo of the client-ui; its `staging` branch may deploy to the staging client bucket + invalidate its CloudFront dist."
  type        = string
  default     = "faisal-energyexe/energyexe-client-ui"
}

# CORS + portal URLs now point at the AWS CloudFront staging hostnames (above),
# wired via tfvars once the certs ISSUE and the host CNAMEs resolve.
variable "cors_origins" {
  description = "BACKEND_CORS_ORIGINS as a JSON-array string for the staging API, e.g. '[\"https://staging-dashboard.energyexe.com\",\"https://staging-app.energyexe.com\"]'. Empty = use the app's built-in default list."
  type        = string
  default     = ""
}

variable "admin_portal_url" {
  description = "Staging admin UI URL (https://staging-dashboard.energyexe.com) for link generation. Empty = unset."
  type        = string
  default     = ""
}

variable "client_portal_url" {
  description = "Staging client UI URL (https://staging-app.energyexe.com) for link generation. Empty = unset."
  type        = string
  default     = ""
}

# --- CI/CD (OIDC) ---

variable "github_backend_repo" {
  description = "owner/repo of the backend; its `staging` branch may deploy the staging service."
  type        = string
  default     = "Energy-Exe/energyexe-core-backend"
}

variable "deploy_branch" {
  description = "Branch whose pushes may deploy to staging."
  type        = string
  default     = "staging"
}
