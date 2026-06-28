# Staging deploy role via GitHub OIDC — SEPARATE from prod's deploy role, trust
# pinned to the backend repo's `staging` branch only. Prod's cicd.tf is
# untouched. The OIDC provider itself is the account-level one prod created
# (data.tf).

locals {
  oidc_arn = data.aws_iam_openid_connect_provider.github.arn
}

# --- Backend: build + push staging image, redeploy the staging service ---

data "aws_iam_policy_document" "backend_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"
    principals {
      type        = "Federated"
      identifiers = [local.oidc_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_backend_repo}:ref:refs/heads/${var.deploy_branch}"]
    }
  }
}

resource "aws_iam_role" "backend_deploy" {
  name               = "${local.name}-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.backend_assume.json
}

resource "aws_iam_role_policy" "backend_deploy" {
  name = "deploy"
  role = aws_iam_role.backend_deploy.id

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

# --- Frontends: sync the built SPA to its staging bucket + invalidate CloudFront ---
# One role per frontend repo, each trust-pinned to that repo's `staging` branch
# and scoped to ITS bucket + distribution only. The active frontend repos live
# under faisal-energyexe (the Energy-Exe org copies are stale).

locals {
  frontend_repos = {
    admin  = var.github_admin_ui_repo
    client = var.github_client_ui_repo
  }
}

data "aws_iam_policy_document" "frontend_assume" {
  for_each = local.frontend_repos
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"
    principals {
      type        = "Federated"
      identifiers = [local.oidc_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${each.value}:ref:refs/heads/${var.deploy_branch}"]
    }
  }
}

resource "aws_iam_role" "frontend_deploy" {
  for_each           = local.frontend_repos
  name               = "energyexe-staging-${each.key}-ui-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.frontend_assume[each.key].json
}

resource "aws_iam_role_policy" "frontend_deploy" {
  for_each = local.frontend_repos
  name     = "deploy"
  role     = aws_iam_role.frontend_deploy[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Sync"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.frontend[each.key].arn,
          "${aws_s3_bucket.frontend[each.key].arn}/*",
        ]
      },
      {
        Sid      = "CloudFrontInvalidate"
        Effect   = "Allow"
        Action   = ["cloudfront:CreateInvalidation", "cloudfront:GetInvalidation"]
        Resource = aws_cloudfront_distribution.frontend[each.key].arn
      },
    ]
  })
}
