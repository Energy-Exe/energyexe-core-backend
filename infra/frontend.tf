# Production frontends on AWS — private S3 (OAC) + CloudFront, one stack per UI,
# replacing Vercel. Mirrors the proven staging pilot (infra/staging/frontend.tf):
#
#   dashboard.energyexe.com -> admin-ui   (deploys from faisal-energyexe/energyexe-admin-ui, branch master)
#   app.energyexe.com       -> client-ui  (deploys from faisal-energyexe/energyexe-client-ui, branch main)
#
# Why Vercel is being replaced: its GitHub push webhooks for production merges
# are unreliable (missed twice on 2026-08-17 alone, each needing an empty-commit
# retrigger), and staging already runs this exact S3+CloudFront shape.
#
# CERT STRATEGY — one WILDCARD cert (*.energyexe.com, us-east-1), NOT per-domain:
# app./dashboard. are currently CNAMEs to vercel-dns-017.com, whose CAA record
# authorizes only letsencrypt/sectigo/pki.goog/globalsign — not Amazon. ACM's
# CAA tree-walk follows those CNAMEs, so per-hostname certs CANNOT issue until
# the Vercel CNAMEs are gone (chicken-and-egg with a zero-downtime cutover).
# A wildcard's CAA check runs against the parent (energyexe.com), which has NO
# CAA record — verified 2026-08-17 — so it issues while Vercel still holds DNS.
# Both dists share the wildcard; the cutover becomes a pure CNAME flip with
# instant rollback (repoint at Vercel).
#
# Two-phase (DNS is manual at hyp.net):
#   Phase 1 — apply with frontend_certificate_arn="": buckets, OAC, dists on the
#             default *.cloudfront.net cert, the wildcard cert request, deploy
#             roles. Output frontend_cert_validation_record; add it at hyp.net.
#   Phase 2 — once the cert is ISSUED, set frontend_certificate_arn in tfvars
#             and re-apply: each dist picks up its alias + the cert (in-place).
#             Verify with curl --resolve <domain>:443:<cloudfront-ip>, then flip
#             the two CNAMEs at hyp.net from vercel-dns to the dist hostnames.
#             After the flip + soak, remove the Vercel projects.

provider "aws" {
  alias   = "us_east_1"
  region  = "us-east-1" # hard CloudFront requirement for viewer certs
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
  prod_frontends = {
    admin = {
      domain = var.admin_ui_domain
      bucket = "energyexe-prod-admin-ui"
      repo   = var.github_admin_ui_repo
      branch = "master" # admin-ui's default/prod branch
    }
    client = {
      domain = var.client_ui_domain
      bucket = "energyexe-prod-client-ui"
      repo   = var.github_client_ui_repo
      branch = "main" # client-ui's default/prod branch
    }
  }
}

# --- Private origin bucket (no public access; CloudFront reaches it via OAC) ---

resource "aws_s3_bucket" "prod_frontend" {
  for_each = local.prod_frontends
  bucket   = each.value.bucket
}

resource "aws_s3_bucket_public_access_block" "prod_frontend" {
  for_each                = local.prod_frontends
  bucket                  = aws_s3_bucket.prod_frontend[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Shared wildcard ACM cert (us-east-1) — see CERT STRATEGY above ---

resource "aws_acm_certificate" "prod_frontend_wildcard" {
  provider          = aws.us_east_1
  domain_name       = "*.energyexe.com"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# --- Origin Access Control: CloudFront signs S3 requests ---

resource "aws_cloudfront_origin_access_control" "prod_frontend" {
  for_each                          = local.prod_frontends
  name                              = "${each.value.bucket}-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "prod_frontend" {
  for_each            = local.prod_frontends
  enabled             = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100" # NA + EU edges; users are EU — bump to PriceClass_All only if far-east latency ever matters
  comment             = "${each.key} production (${each.value.domain})"

  # Alias attaches only in phase 2 (cert ISSUED + frontend_certificate_arn set).
  aliases = var.frontend_certificate_arn == "" ? [] : [each.value.domain]

  origin {
    domain_name              = aws_s3_bucket.prod_frontend[each.key].bucket_regional_domain_name
    origin_id                = "s3-${each.key}"
    origin_access_control_id = aws_cloudfront_origin_access_control.prod_frontend[each.key].id
  }

  default_cache_behavior {
    target_origin_id       = "s3-${each.key}"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true
    # AWS-managed "CachingOptimized" policy. Vite assets are content-hashed so
    # long edge TTLs are safe; index.html freshness comes from the deploy
    # workflow's /* invalidation (same contract as staging).
    cache_policy_id = "658327ea-f89d-4fab-a63d-7e88639e58f6"
  }

  # SPA client-side routing: rewrite S3's 403/404 for deep links to /index.html
  # (the AWS equivalent of both repos' vercel.json rewrite — their ONLY config).
  custom_error_response {
    error_code            = 403
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 10
  }
  custom_error_response {
    error_code            = 404
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 10
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = var.frontend_certificate_arn == "" ? true : null
    acm_certificate_arn            = var.frontend_certificate_arn == "" ? null : var.frontend_certificate_arn
    ssl_support_method             = var.frontend_certificate_arn == "" ? null : "sni-only"
    minimum_protocol_version       = var.frontend_certificate_arn == "" ? null : "TLSv1.2_2021"
  }
}

# --- Bucket policy: only this CloudFront distribution may read objects ---

data "aws_iam_policy_document" "prod_frontend_s3" {
  for_each = local.prod_frontends
  statement {
    sid       = "AllowCloudFrontServicePrincipalReadOnly"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.prod_frontend[each.key].arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.prod_frontend[each.key].arn]
    }
  }
}

resource "aws_s3_bucket_policy" "prod_frontend" {
  for_each = local.prod_frontends
  bucket   = aws_s3_bucket.prod_frontend[each.key].id
  policy   = data.aws_iam_policy_document.prod_frontend_s3[each.key].json
}

# --- Deploy roles via GitHub OIDC — one per frontend repo, trust-pinned to its
# --- prod branch, scoped to ITS bucket + distribution only (staging pattern).

data "aws_iam_policy_document" "prod_frontend_assume" {
  for_each = local.prod_frontends
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
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${each.value.repo}:ref:refs/heads/${each.value.branch}"]
    }
  }
}

resource "aws_iam_role" "prod_frontend_deploy" {
  for_each           = local.prod_frontends
  name               = "energyexe-prod-${each.key}-ui-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.prod_frontend_assume[each.key].json
}

resource "aws_iam_role_policy" "prod_frontend_deploy" {
  for_each = local.prod_frontends
  name     = "deploy"
  role     = aws_iam_role.prod_frontend_deploy[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Sync"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.prod_frontend[each.key].arn,
          "${aws_s3_bucket.prod_frontend[each.key].arn}/*",
        ]
      },
      {
        Sid      = "CloudFrontInvalidate"
        Effect   = "Allow"
        Action   = ["cloudfront:CreateInvalidation", "cloudfront:GetInvalidation"]
        Resource = aws_cloudfront_distribution.prod_frontend[each.key].arn
      },
    ]
  })
}
