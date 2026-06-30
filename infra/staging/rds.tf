# Staging database: a SEPARATE RDS instance restored from a prod snapshot.
#
# Private (unlike prod's public energyexedb) — only the in-VPC staging task
# reaches it. Storage stays at the snapshot's allocated size (~200GB) — a
# restore can't shrink it. Engine version, master username, and password are
# inherited from the snapshot, so the database-url secret uses prod's master
# credentials against this host (rotate post-restore if desired).

resource "aws_db_subnet_group" "staging" {
  name       = "energyexedb-staging"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_db_instance" "staging" {
  identifier          = "energyexedb-staging"
  snapshot_identifier = var.rds_snapshot_identifier

  instance_class       = var.rds_instance_class
  storage_type         = "gp3"
  storage_encrypted    = true # inherited from the prod snapshot
  db_subnet_group_name = aws_db_subnet_group.staging.name
  parameter_group_name = "default.postgres17"

  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = false

  # Staging is disposable: allow teardown + skip the final snapshot.
  deletion_protection = false
  skip_final_snapshot = true
  apply_immediately   = true

  # Keep auto minor upgrades off so staging tracks prod's version.
  auto_minor_version_upgrade = false

  # snapshot_identifier only matters at creation; ignore it post-import so a set
  # value doesn't force a destructive replace. To refresh staging data, restore
  # a new instance explicitly rather than via this resource.
  lifecycle {
    ignore_changes = [snapshot_identifier]
  }
}
