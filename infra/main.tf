terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Remote state — recommended once past the first apply:
  #   1. aws s3 mb s3://energyexe-terraform-state --profile energyexe --region eu-north-1
  #   2. Uncomment below, then run: terraform init -migrate-state
  # backend "s3" {
  #   bucket       = "energyexe-terraform-state"
  #   key          = "core-backend/terraform.tfstate"
  #   region       = "eu-north-1"
  #   profile      = "energyexe"
  #   use_lockfile = true
  # }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = "energyexe"
      Service   = "core-backend"
      ManagedBy = "terraform"
    }
  }
}

locals {
  name           = "energyexe-core-backend"
  container_name = "api"
  container_port = 8001
}
