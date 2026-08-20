output "api_url" {
  description = "API base URL (ALB DNS). CNAME your api domain to this once you add a cert."
  value       = "http://${aws_lb.this.dns_name}"
}

output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "ecr_repository_url" {
  description = "Push the production image here"
  value       = aws_ecr_repository.this.repository_url
}

output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "service_name" {
  value = aws_ecs_service.api.name
}

output "valkey_endpoint" {
  description = "VPC-only Valkey endpoint (not reachable from your laptop; local dev keeps using docker-compose redis)"
  value       = "${aws_elasticache_serverless_cache.valkey.endpoint[0].address}:${aws_elasticache_serverless_cache.valkey.endpoint[0].port}"
}

output "log_group" {
  description = "Tail with: aws logs tail /ecs/energyexe-core-backend --follow --profile energyexe"
  value       = aws_cloudwatch_log_group.this.name
}

# --- Production frontends (frontend.tf) ---

output "frontend_cert_validation_record" {
  description = "Add this CNAME at hyp.net to DNS-validate the *.energyexe.com cert (phase 1 -> 2)."
  value = {
    for o in aws_acm_certificate.prod_frontend_wildcard.domain_validation_options :
    o.domain_name => { name = o.resource_record_name, type = o.resource_record_type, value = o.resource_record_value }
  }
}

output "frontend_distributions" {
  description = "Per-frontend CloudFront dist id + hostname. The hostname is the CNAME target for the prod cutover at hyp.net."
  value = {
    for k, d in aws_cloudfront_distribution.prod_frontend :
    k => { id = d.id, domain = d.domain_name, alias = local.prod_frontends[k].domain }
  }
}

output "frontend_deploy_role_arns" {
  description = "GitHub OIDC deploy roles for the frontend repos' prod deploy workflows."
  value       = { for k, r in aws_iam_role.prod_frontend_deploy : k => r.arn }
}
