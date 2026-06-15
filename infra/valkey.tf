# ElastiCache Serverless (Valkey) — replaces the Railway Valkey instance.
#
# The app's only consumer today is the windfarm report cache (1h TTL) in
# app/api/v1/endpoints/windfarm_reports.py, and app/core/redis.py degrades
# gracefully to no-cache if unreachable.
#
# ~$6–7/mo at the 100 MB billing floor; the usage limits below cap the worst
# case at ~1 GB. Zero-cost alternative if even that feels like too much: run a
# valkey container as a sidecar in the ECS task (cache wiped on every deploy,
# which a 1h-TTL cache tolerates fine).
#
# Serverless ElastiCache requires TLS — hence the rediss:// URL in ecs.tf.

resource "aws_security_group" "valkey" {
  name_prefix = "${local.name}-valkey-"
  description = "Valkey serverless cache for ${local.name}"
  vpc_id      = data.aws_vpc.default.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "valkey_from_service" {
  security_group_id            = aws_security_group.valkey.id
  description                  = "Valkey from ${local.name} ECS service"
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = 6379
  to_port                      = 6379
  ip_protocol                  = "tcp"
}

resource "aws_elasticache_serverless_cache" "valkey" {
  name                 = "${local.name}-cache"
  engine               = "valkey"
  major_engine_version = "8"
  security_group_ids   = [aws_security_group.valkey.id]
  subnet_ids           = data.aws_subnets.default.ids

  cache_usage_limits {
    data_storage {
      maximum = 1
      unit    = "GB"
    }
    ecpu_per_second {
      maximum = 1000
    }
  }
}
