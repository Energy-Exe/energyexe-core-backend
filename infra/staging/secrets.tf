# Staging-specific secret CONTAINERS only — values set out-of-band so they never
# land in Terraform state:
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/core-backend-staging/database-url \
#     --secret-string 'postgresql+asyncpg://postgres:PASS@energyexedb-staging.<id>.eu-north-1.rds.amazonaws.com:5432/energyexe'
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/core-backend-staging/secret-key \
#     --secret-string "$(openssl rand -hex 32)"   # NEW key — keeps staging JWTs distinct from prod
#
#   aws secretsmanager put-secret-value --profile energyexe \
#     --secret-id energyexe/core-backend-staging/sentry-dsn \
#     --secret-string 'https://<key>@errors.energyexe.com/<staging_project_id>'
#
# The external-API-key + brain-agent secrets are REUSED from prod (see data.tf).

resource "aws_secretsmanager_secret" "database_url" {
  name        = "energyexe/core-backend-staging/database-url"
  description = "Async SQLAlchemy URL for the staging RDS (postgresql+asyncpg://...)"
}

resource "aws_secretsmanager_secret" "secret_key" {
  name        = "energyexe/core-backend-staging/secret-key"
  description = "JWT signing key for staging (distinct from prod)"
}

resource "aws_secretsmanager_secret" "sentry_dsn" {
  name        = "energyexe/core-backend-staging/sentry-dsn"
  description = "GlitchTip DSN for the staging backend (staging project, SENTRY_ENVIRONMENT=staging)"
}
