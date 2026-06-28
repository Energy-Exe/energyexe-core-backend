# Staging environment — a SEPARATE Terraform root from prod (../).
#
# Its state lives here (infra/staging/terraform.tfstate) and never references
# prod's state, so a `terraform apply` in this directory cannot create, modify,
# or destroy any prod-managed resource. Shared prod infra (ALB, ECS cluster,
# VPC) is consumed read-only via data sources; everything else is staging-owned.
#
# See ../infra (the prod root) for the resources this mirrors.

terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = "energyexe"
      Service   = "core-backend"
      Env       = "staging"
      ManagedBy = "terraform"
    }
  }
}

# CloudFront ACM certificates MUST be issued in us-east-1, regardless of where
# the rest of the stack lives. Used only by the frontend distributions.
provider "aws" {
  alias   = "us_east_1"
  region  = "us-east-1"
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = "energyexe"
      Service   = "core-backend"
      Env       = "staging"
      ManagedBy = "terraform"
    }
  }
}

locals {
  name           = "energyexe-core-backend-staging"
  container_name = "api"
  container_port = 8001
}
