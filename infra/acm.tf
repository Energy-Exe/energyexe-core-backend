# TLS certificate for the API's custom domain.
#
# energyexe.com DNS is hosted at hyp.net (NOT Route53), so validation records
# can't be auto-created — Terraform provisions the cert in PENDING_VALIDATION
# and the acm_validation_records output tells you exactly what CNAME to add at
# hyp.net. Once added and the cert flips to ISSUED, set certificate_arn (see
# the acm_certificate_arn output) and re-apply to wire up the HTTPS listener.
#
# Cert lives in eu-north-1 (same region as the ALB), which is what an ALB
# requires.

variable "api_domain" {
  description = "Custom domain for the API, e.g. api.energyexe.com. Empty = skip cert creation."
  type        = string
  default     = ""
}

resource "aws_acm_certificate" "api" {
  count             = var.api_domain == "" ? 0 : 1
  domain_name       = var.api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

output "acm_certificate_arn" {
  description = "Set this as certificate_arn in terraform.tfvars once the cert is ISSUED, then re-apply to add the HTTPS listener."
  value       = var.api_domain == "" ? "" : aws_acm_certificate.api[0].arn
}

output "acm_validation_records" {
  description = "Add these CNAME record(s) at hyp.net to validate the certificate."
  value = var.api_domain == "" ? [] : [
    for o in aws_acm_certificate.api[0].domain_validation_options : {
      add_cname_named = o.resource_record_name
      with_value      = o.resource_record_value
    }
  ]
}

output "api_cname_target" {
  description = "After the cert is ISSUED and the HTTPS listener exists, point api_domain here with a CNAME at hyp.net."
  value       = aws_lb.this.dns_name
}
