# Staging rides the SHARED prod ALB. We add only: a NEW target group, an SNI
# cert, and an additive host rule on prod's :443 listener. We never modify the
# listener's default action, prod's rule, or prod's target group.
#
# Two-phase (like prod, because DNS is manual at hyp.net): phase 1 creates the
# cert + TG and outputs the validation record; after the CNAME is added and the
# cert ISSUES, set staging_api_certificate_arn in tfvars to wire the host rule.

resource "aws_lb_target_group" "api" {
  name        = "energyexe-staging-tg" # <=32 chars; "${local.name}-tg" would overflow
  port        = local.container_port
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  health_check {
    path                = "/health"
    matcher             = "200"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 5
  }

  deregistration_delay = 30
}

resource "aws_acm_certificate" "staging_api" {
  domain_name       = var.staging_api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# The host rule is created in phase 1 so the target group is associated with the
# ALB (ECS requires this to attach a service). It's additive + host-scoped to
# staging-api, so it never affects api.* / errors.* traffic. Until the SNI cert
# below is attached (phase 2), HTTPS for this host falls back to the ALB default
# cert (a mismatch warning) — harmless, since DNS isn't pointed here until then.
resource "aws_lb_listener_rule" "staging_api" {
  listener_arn = data.aws_lb_listener.prod_https.arn
  priority     = 110 # GlitchTip is 100; default action stays prod's backend

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }

  condition {
    host_header {
      values = [var.staging_api_domain]
    }
  }
}

# Phase 2 (gated on the issued cert): attach the staging-api cert via SNI so the
# host serves a valid certificate.
resource "aws_lb_listener_certificate" "staging_api" {
  count           = var.staging_api_certificate_arn == "" ? 0 : 1
  listener_arn    = data.aws_lb_listener.prod_https.arn
  certificate_arn = var.staging_api_certificate_arn
}
