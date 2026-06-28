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
