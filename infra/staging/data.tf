# Shared infra consumed READ-ONLY. Data sources never modify their targets, so
# nothing here can affect prod.

data "aws_caller_identity" "current" {}

# Default VPC — same one prod + energyexedb live in.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Prod ALB + its HTTPS listener. Staging adds an (additive) host rule + SNI cert
# to this listener from its own state; it never edits the listener default
# action or prod's existing rules.
data "aws_lb" "prod" {
  name = var.prod_alb_name
}

data "aws_lb_listener" "prod_https" {
  load_balancer_arn = data.aws_lb.prod.arn
  port              = 443
}

# Shared ECS cluster (adding a service to it cannot affect prod's service).
data "aws_ecs_cluster" "shared" {
  cluster_name = var.shared_cluster_name
}

# Account-level GitHub OIDC provider — already created by the prod root; staging
# references it rather than creating a duplicate (which would conflict).
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

# Prod's external-API-key + brain-agent secrets, reused by staging (the brain
# agent's DB roles exist in the restored snapshot, so the same passwords work).
locals {
  reused_secret_names = [
    "anthropic-api-key",
    "brain-agent-ro-password",
    "brain-agent-client-ro-password",
    "cdsapi-key",
    "eia-api-key",
    "entsoe-api-key",
    "github-token",
    "resend-api-key",
  ]
  # env var name the app reads, keyed to the prod secret short-name.
  reused_secret_env = {
    ANTHROPIC_API_KEY              = "anthropic-api-key"
    BRAIN_AGENT_RO_PASSWORD        = "brain-agent-ro-password"
    BRAIN_AGENT_CLIENT_RO_PASSWORD = "brain-agent-client-ro-password"
    CDSAPI_KEY                     = "cdsapi-key"
    EIA_API_KEY                    = "eia-api-key"
    ENTSOE_API_KEY                 = "entsoe-api-key"
    GITHUB_TOKEN                   = "github-token"
    RESEND_API_KEY                 = "resend-api-key"
  }
}

data "aws_secretsmanager_secret" "reused" {
  for_each = toset(local.reused_secret_names)
  name     = "energyexe/core-backend/${each.value}"
}
