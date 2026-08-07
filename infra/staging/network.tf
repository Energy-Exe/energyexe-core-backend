# Staging security groups. The ALB itself is shared (prod-owned); staging only
# creates its own service + RDS SGs.

# --- ECS service: reachable only from the shared prod ALB ---

resource "aws_security_group" "service" {
  name_prefix = "${local.name}-svc-"
  description = "Fargate tasks for ${local.name}"
  vpc_id      = data.aws_vpc.default.id

  lifecycle {
    create_before_destroy = true
  }
}

# Inbound app traffic comes from the prod ALB's security group (staging rides
# that ALB). Referencing prod's ALB SG is read-only — we add a rule to OUR SG.
resource "aws_vpc_security_group_ingress_rule" "service_from_alb" {
  security_group_id            = aws_security_group.service.id
  description                  = "App traffic from the shared ALB"
  referenced_security_group_id = tolist(data.aws_lb.prod.security_groups)[0]
  from_port                    = local.container_port
  to_port                      = local.container_port
  ip_protocol                  = "tcp"
}

# Outbound open: ECR pulls, Secrets Manager, CloudWatch Logs, external data APIs.
resource "aws_vpc_security_group_egress_rule" "service_all" {
  security_group_id = aws_security_group.service.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- Staging RDS: Postgres reachable only from the staging service ---

resource "aws_security_group" "rds" {
  # NOTE: description is immutable (changing it REPLACES the SG — disruptive
  # on a live RDS). It predates the 2026-07-15 public flip; rules below are
  # the source of truth for the actual posture.
  name_prefix = "${local.name}-rds-"
  description = "Staging RDS - Postgres from the staging ECS service only"
  vpc_id      = data.aws_vpc.default.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_service" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from the staging ECS service"
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# Staging is the laptop/dev database since 2026-07-15 (prod went private).
# Open ingress is a deliberate user decision; mitigations: rotated master
# password (2026-07-15), rds.force_ssl=1, and this is the only public DB.
resource "aws_vpc_security_group_ingress_rule" "rds_public" {
  security_group_id = aws_security_group.rds.id
  description       = "Postgres open to internet (user decision 2026-07-15; rotated pw + force_ssl)"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 5432
  to_port           = 5432
  ip_protocol       = "tcp"
}
