# master -> prod image promotion (Phase 2).
#
# Prod's deploy workflow (.github/workflows/deploy-aws.yml) no longer rebuilds on
# master: it PROMOTES the exact image already validated in staging by copying it
# from THIS staging ECR repo into the prod repo, then force-new-deployment.
#
# For that copy, the PROD deploy role needs to PULL from the staging repo. We
# grant it here, from the STAGING side, via an ECR repository policy — so prod's
# IAM (../cicd.tf) stays untouched. Same-account ECR access can be granted by the
# repository policy alone (the prod role already has registry-level
# GetAuthorizationToken + push on its own repo).

data "aws_iam_role" "prod_deploy" {
  name = "energyexe-core-backend-github-deploy" # prod root: "${local.name}-github-deploy"
}

resource "aws_ecr_repository_policy" "allow_prod_promote_pull" {
  repository = aws_ecr_repository.this.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowProdDeployRolePullForPromotion"
      Effect    = "Allow"
      Principal = { AWS = data.aws_iam_role.prod_deploy.arn }
      Action = [
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchCheckLayerAvailability",
      ]
    }]
  })
}
