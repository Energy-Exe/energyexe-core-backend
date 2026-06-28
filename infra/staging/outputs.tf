# --- DNS records to add manually at hyp.net (backend only; frontends are on Vercel) ---

output "staging_api_cert_validation" {
  description = "CNAME to add so the staging-api ACM cert validates (already ISSUED if record 1 is in)."
  value = [for o in aws_acm_certificate.staging_api.domain_validation_options : {
    name  = o.resource_record_name
    type  = o.resource_record_type
    value = o.resource_record_value
  }]
}

output "staging_api_certificate_arn" {
  description = "ARN to copy into staging_api_certificate_arn once the cert ISSUES (phase 2)."
  value       = aws_acm_certificate.staging_api.arn
}

output "staging_api_cname_target" {
  description = "Point staging-api.energyexe.com (CNAME) at the shared ALB."
  value       = data.aws_lb.prod.dns_name
}

# --- Identifiers used by CI / secret population ---

output "staging_rds_endpoint" {
  description = "Host:port for the staging database-url secret."
  value       = aws_db_instance.staging.endpoint
}

output "staging_ecr_repository_url" {
  description = "Staging ECR repo the backend staging workflow pushes to."
  value       = aws_ecr_repository.this.repository_url
}

output "backend_deploy_role_arn" {
  description = "AWS_DEPLOY_ROLE_ARN for the backend repo's staging workflow."
  value       = aws_iam_role.backend_deploy.arn
}

# --- Staging frontends on AWS (S3 + CloudFront) ---

# Phase 1: add these CNAMEs at hyp.net so the us-east-1 frontend certs validate.
output "frontend_cert_validation" {
  description = "Per-frontend ACM validation CNAMEs to add at hyp.net (admin + client)."
  value = {
    for k, cert in aws_acm_certificate.frontend : k => [
      for o in cert.domain_validation_options : {
        name  = o.resource_record_name
        type  = o.resource_record_type
        value = o.resource_record_value
      }
    ]
  }
}

# Phase 2: copy these into admin_certificate_arn / client_certificate_arn once ISSUED.
output "frontend_certificate_arns" {
  description = "ARNs to set in tfvars (admin_certificate_arn / client_certificate_arn) after the certs ISSUE."
  value       = { for k, c in aws_acm_certificate.frontend : k => c.arn }
}

# Service CNAMEs: point staging-dashboard / staging-app at these CloudFront domains.
output "frontend_cname_targets" {
  description = "Point each staging frontend host (CNAME) at its CloudFront domain."
  value = {
    for k, d in aws_cloudfront_distribution.frontend :
    local.frontends[k].domain => d.domain_name
  }
}

# Set as the CLOUDFRONT_DISTRIBUTION_ID repo var in each frontend's staging CI.
output "frontend_distribution_ids" {
  description = "CloudFront distribution IDs (for the deploy workflow's invalidation step)."
  value       = { for k, d in aws_cloudfront_distribution.frontend : k => d.id }
}

output "frontend_buckets" {
  description = "Origin S3 buckets the staging frontend CI syncs the build into."
  value       = { for k, b in aws_s3_bucket.frontend : k => b.id }
}

output "frontend_deploy_role_arns" {
  description = "AWS_DEPLOY_ROLE_ARN for each frontend repo's staging workflow."
  value       = { for k, r in aws_iam_role.frontend_deploy : k => r.arn }
}
