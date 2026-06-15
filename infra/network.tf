# Reuse the default VPC — it's where energyexedb already lives
# (vpc-04f8e9553ad77d458, all subnets public, no NAT gateway needed).

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# --- ALB: open to the world on 80/443 ---

resource "aws_security_group" "alb" {
  name_prefix = "${local.name}-alb-"
  description = "ALB for ${local.name}"
  vpc_id      = data.aws_vpc.default.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTP"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "alb_https" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTPS"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "alb_all" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- ECS service: only reachable from the ALB ---

resource "aws_security_group" "service" {
  name_prefix = "${local.name}-svc-"
  description = "Fargate tasks for ${local.name}"
  vpc_id      = data.aws_vpc.default.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "service_from_alb" {
  security_group_id            = aws_security_group.service.id
  description                  = "App traffic from ALB"
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = local.container_port
  to_port                      = local.container_port
  ip_protocol                  = "tcp"
}

# Outbound open: ECR image pulls, Secrets Manager, CloudWatch Logs, and the
# external data APIs (ENTSOE, Elexon, EIA, Taipower, NVE, CDS).
resource "aws_vpc_security_group_egress_rule" "service_all" {
  security_group_id = aws_security_group.service.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- Existing RDS security group: allow Postgres from the service ---
# In-VPC traffic resolves to the RDS private IP, so SG-to-SG referencing works.

resource "aws_vpc_security_group_ingress_rule" "rds_from_service" {
  security_group_id            = var.rds_security_group_id
  description                  = "Postgres from ${local.name} ECS service"
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}
