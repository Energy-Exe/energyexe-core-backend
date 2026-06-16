# GitHub Actions -> AWS via OIDC. No long-lived keys stored in the repo:
# the workflow exchanges a short-lived GitHub OIDC token for temporary AWS
# credentials by assuming the role below. Trust is pinned to this repo's
# master branch only.

data "aws_caller_identity" "current" {}

variable "github_repo" {
  description = "owner/repo permitted to assume the deploy role"
  type        = string
  default     = "Energy-Exe/energyexe-core-backend"
}

variable "deploy_branch" {
  description = "Branch whose pushes may deploy"
  type        = string
  default     = "master"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Only this repo + branch. Tighten/loosen via the variables above.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:ref:refs/heads/${var.deploy_branch}"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name               = "${local.name}-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

# Exactly what the deploy workflow does: auth to ECR, push the image, force a
# new ECS deployment, and poll the service until stable. Nothing more — it
# can't read secrets, touch RDS, or register new task definitions.
resource "aws_iam_role_policy" "github_deploy" {
  name = "deploy"
  role = aws_iam_role.github_deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "EcrAuth"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Sid    = "EcrPush"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
        ]
        Resource = aws_ecr_repository.this.arn
      },
      {
        Sid      = "EcsDeploy"
        Effect   = "Allow"
        Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
        Resource = aws_ecs_service.api.id
      },
    ]
  })
}

output "github_deploy_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN repo variable (or hardcode in the workflow)."
  value       = aws_iam_role.github_deploy.arn
}
