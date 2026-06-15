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
