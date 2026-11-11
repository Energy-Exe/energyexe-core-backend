# Staging frontends hosted on AWS — private S3 (OAC) + CloudFront, one stack per
# UI. This is the S3+CloudFront pilot from the plan (replacing Vercel for the
# staging frontends). Both use DIRECT-CHILD hostnames of energyexe.com:
#
#   staging-dashboard.energyexe.com -> admin-ui
#   staging-app.energyexe.com       -> client-ui
#
# Why direct-child and NOT staging.dashboard.* / staging.app.*: those labels sit
# under dashboard.* / app.*, which are CNAMEs to Vercel, and Vercel's CAA record
# authorizes only letsencrypt/sectigo/pki.goog/globalsign — NOT Amazon. ACM's
# CAA tree-walk follows that CNAME and refuses to issue. A direct child of
# energyexe.com has no Vercel label in its path (proven by staging-api), so ACM
# issues cleanly.
#
# Two-phase (like the rest of this root, because DNS is manual at hyp.net):
#   Phase 1 — apply with empty *_certificate_arn: creates the S3 bucket, the
#             us-east-1 ACM cert, and a CloudFront dist on the *.cloudfront.net
#             default cert (no custom alias yet). Outputs the validation CNAME.
#   Phase 2 — add the validation CNAME at hyp.net; once the cert ISSUES, set
#             admin_certificate_arn / client_certificate_arn in tfvars and
#             re-apply: CloudFront picks up the alias + the ACM cert (in-place
#             update, not a replace). Then point the host CNAME at CloudFront.

locals {
  frontends = {
    admin = {
      domain = var.admin_staging_domain # staging-dashboard.energyexe.com (admin-ui)
      bucket = "energyexe-staging-admin-ui"
      cert   = var.admin_certificate_arn
    }
    client = {
      domain = var.client_staging_domain # staging-app.energyexe.com (client-ui)
      bucket = "energyexe-staging-client-ui"
      cert   = var.client_certificate_arn
    }
  }
}

# --- Private origin bucket (no public access; CloudFront reaches it via OAC) ---

resource "aws_s3_bucket" "frontend" {
  for_each = local.frontends
  bucket   = each.value.bucket
}

resource "aws_s3_bucket_public_access_block" "frontend" {
  for_each                = local.frontends
  bucket                  = aws_s3_bucket.frontend[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- ACM cert (us-east-1 is a hard CloudFront requirement) ---

resource "aws_acm_certificate" "frontend" {
  for_each          = local.frontends
  provider          = aws.us_east_1
  domain_name       = each.value.domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# --- Origin Access Control: CloudFront signs S3 requests (replaces legacy OAI) ---

resource "aws_cloudfront_origin_access_control" "frontend" {
  for_each                          = local.frontends
  name                              = "${each.value.bucket}-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "frontend" {
  for_each            = local.frontends
  enabled             = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100" # NA + EU edges only — cheapest, fine for staging
  comment             = "${each.key} staging (${each.value.domain})"

  # Alias is only attached once the cert is ISSUED (phase 2). Empty in phase 1 so
  # the dist serves on its *.cloudfront.net default cert.
  aliases = each.value.cert == "" ? [] : [each.value.domain]

  origin {
    domain_name              = aws_s3_bucket.frontend[each.key].bucket_regional_domain_name
    origin_id                = "s3-${each.key}"
    origin_access_control_id = aws_cloudfront_origin_access_control.frontend[each.key].id
  }

  # PostHog origins (client portal only); see the PostHog reverse proxy note above.
  dynamic "origin" {
    for_each = each.key == "client" ? {
      posthog-ingest = local.posthog_ingest_host
      posthog-assets = local.posthog_assets_host
    } : {}
    content {
      domain_name = origin.value
      origin_id   = origin.key
      custom_origin_config {
        http_port              = 80
        https_port             = 443
        origin_protocol_policy = "https-only"
        origin_ssl_protocols   = ["TLSv1.2"]
      }
    }
  }

  # Most specific first: scripts + remote config from the assets host, the rest
  # (events, recordings, flags) from the ingestion host.
  dynamic "ordered_cache_behavior" {
    for_each = each.key == "client" ? [
      { path = "/ingest/static/*", origin = "posthog-assets" },
      { path = "/ingest/array/*", origin = "posthog-assets" },
      { path = "/ingest/*", origin = "posthog-ingest" },
    ] : []
    content {
      path_pattern             = ordered_cache_behavior.value.path
      target_origin_id         = ordered_cache_behavior.value.origin
      viewer_protocol_policy   = "redirect-to-https"
      allowed_methods          = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
      cached_methods           = ["GET", "HEAD"]
      compress                 = false
      cache_policy_id          = local.cf_cache_disabled
      origin_request_policy_id = local.cf_origin_all_viewer_except_host
      function_association {
        event_type   = "viewer-request"
        function_arn = aws_cloudfront_function.posthog_ingest.arn
      }
    }
  }

  default_cache_behavior {
    target_origin_id       = "s3-${each.key}"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true
    # AWS-managed "CachingOptimized" policy (no cookies/headers/query forwarded).
    cache_policy_id = "658327ea-f89d-4fab-a63d-7e88639e58f6"
  }

  # SPA client-side routing: S3 returns 403/404 for unknown deep-link paths;
  # rewrite both to /index.html with a 200 so the router takes over (the AWS
  # equivalent of the Vercel SPA rewrite).
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
    cloudfront_default_certificate = each.value.cert == "" ? true : null
    acm_certificate_arn            = each.value.cert == "" ? null : each.value.cert
    ssl_support_method             = each.value.cert == "" ? null : "sni-only"
    minimum_protocol_version       = each.value.cert == "" ? null : "TLSv1.2_2021"
  }
}

# --- PostHog reverse proxy (client portal only) ---
# The client portal sends analytics to its own address, /ingest/*, and this
# distribution forwards it to PostHog EU. Ad blockers and company firewalls
# block eu.i.posthog.com, but not the portal's own domain. The CloudFront
# Function strips the /ingest prefix. /ingest/static/* and /ingest/array/*
# (scripts and the project's remote config, which recordings need) come from
# PostHog's assets host; everything else (events, recordings, flags) from the
# ingestion host. Nothing is cached and every header, cookie and query string
# is forwarded, except Host, which CloudFront sets to the PostHog origin.
# Note: the SPA custom_error_response (403/404 -> index.html) is
# distribution-wide, so a PostHog 403/404 reaches the browser as index.html.
# posthog-js treats both as failures, so it's harmless.
locals {
  posthog_ingest_host = "eu.i.posthog.com"
  posthog_assets_host = "eu-assets.i.posthog.com"
  # AWS-managed policies
  cf_cache_disabled                = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad" # CachingDisabled
  cf_origin_all_viewer_except_host = "b689b0a8-53d0-40ab-baf2-68738e2966ac" # AllViewerExceptHostHeader
}

resource "aws_cloudfront_function" "posthog_ingest" {
  name    = "energyexe-staging-posthog-ingest"
  runtime = "cloudfront-js-2.0"
  comment = "Strip /ingest before forwarding to PostHog EU (client portal)"
  publish = true
  code    = file("${path.module}/../cloudfront/posthog_ingest.js")
}

# --- Bucket policy: only this CloudFront distribution may read objects (OAC) ---

data "aws_iam_policy_document" "frontend_s3" {
  for_each = local.frontends
  statement {
    sid       = "AllowCloudFrontServicePrincipalReadOnly"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.frontend[each.key].arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.frontend[each.key].arn]
    }
  }
}

resource "aws_s3_bucket_policy" "frontend" {
  for_each = local.frontends
  bucket   = aws_s3_bucket.frontend[each.key].id
  policy   = data.aws_iam_policy_document.frontend_s3[each.key].json
}
