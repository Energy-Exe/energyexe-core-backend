# Secret CONTAINERS only — values are set out-of-band so they never land in
# Terraform state:
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/core-backend/database-url \
#     --secret-string 'postgresql+asyncpg://USER:PASS@energyexedb.<id>.eu-north-1.rds.amazonaws.com:5432/DBNAME'
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/core-backend/secret-key \
#     --secret-string "$(openssl rand -hex 32)"
#
# NOTE: generating a new SECRET_KEY invalidates all existing JWTs (users must
# log in again). Reuse the current Railway value if you want a seamless cutover.

resource "aws_secretsmanager_secret" "database_url" {
  name        = "energyexe/core-backend/database-url"
  description = "Async SQLAlchemy URL for energyexedb (postgresql+asyncpg://...)"
}

resource "aws_secretsmanager_secret" "secret_key" {
  name        = "energyexe/core-backend/secret-key"
  description = "JWT signing key for energyexe-core-backend"
}

# GlitchTip DSN the backend reports errors to. Container always exists so it can
# be populated once GlitchTip is up and a project is created; the backend only
# *reads* it when var.backend_sentry_dsn_enabled=true (ecs.tf).
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/core-backend/sentry-dsn \
#     --secret-string 'https://<public_key>@errors.energyexe.com/<project_id>'
resource "aws_secretsmanager_secret" "backend_sentry_dsn" {
  name        = "energyexe/core-backend/sentry-dsn"
  description = "GlitchTip DSN for the backend's Sentry SDK"
}

# Remaining app secrets, keyed by the env var name the app reads.
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are deliberately absent: the task
# role grants S3 access directly (iam.tf), so no static keys in the container.
locals {
  extra_secrets = {
    ANTHROPIC_API_KEY       = "anthropic-api-key"
    BRAIN_AGENT_RO_PASSWORD = "brain-agent-ro-password"
    CDSAPI_KEY              = "cdsapi-key"
    EIA_API_KEY             = "eia-api-key"
    ENTSOE_API_KEY          = "entsoe-api-key"
    GITHUB_TOKEN            = "github-token"
    RESEND_API_KEY          = "resend-api-key"
  }
}

resource "aws_secretsmanager_secret" "extra" {
  for_each    = local.extra_secrets
  name        = "energyexe/core-backend/${each.value}"
  description = "${each.key} for energyexe-core-backend"
}
