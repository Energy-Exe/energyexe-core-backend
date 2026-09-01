# Partner inbound — S3 drop zone for external SCADA data deliveries.
#
# Partners (first: SFE, Sogn og Fjordane Energi — ~60 SCADA datasets, 5-min
# intervals, one turbine to start) upload raw files here with scoped write-only
# IAM credentials via Cyberduck/WinSCP/AWS CLI. Deliberately a SEPARATE bucket
# from the lake (energyexe-scada-data): external credentials must never be able
# to touch bronze/silver/gold. The scada pipeline copies verified files into
# s3://energyexe-scada-data/bronze/landing/<source>/ from here.
#
# Why not AWS Transfer Family (SFTP): ~$216/mo for the endpoint alone vs $0 for
# plain S3 access — revisit only if a partner's IT mandates SFTP.
#
# Layout: one top-level prefix per partner (sfe/, ...). One IAM user per
# partner, allowed to Put/Get/List ONLY under its own prefix; no delete, so
# uploads are immutable from the uploader's side (versioning covers overwrite).
#
# ⚠️ Access keys are created OUT-OF-BAND, never in Terraform (keys would land in
# state): aws iam create-access-key --user-name sfe-upload --profile energyexe
# Handover to the partner = rotate the key (delete + create), not a new user.
# Uploader instructions live in docs/partner-upload-sfe.md.

resource "aws_s3_bucket" "partner_inbound" {
  bucket = "energyexe-partner-inbound"
}

# Uploader has no s3:DeleteObject, so versioning is the overwrite safety net.
resource "aws_s3_bucket_versioning" "partner_inbound" {
  bucket = aws_s3_bucket.partner_inbound.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "partner_inbound" {
  bucket = aws_s3_bucket.partner_inbound.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "partner_inbound" {
  bucket                  = aws_s3_bucket.partner_inbound.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "partner_inbound" {
  bucket = aws_s3_bucket.partner_inbound.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_policy" "partner_inbound" {
  bucket = aws_s3_bucket.partner_inbound.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.partner_inbound.arn,
          "${aws_s3_bucket.partner_inbound.arn}/*",
        ]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      }
    ]
  })

  # BlockPublicPolicy rejects policies on buckets without the block in place first.
  depends_on = [aws_s3_bucket_public_access_block.partner_inbound]
}

# --- SFE uploader: Put/Get/List under sfe/ only, no delete ---

resource "aws_iam_user" "sfe_upload" {
  name = "sfe-upload"
}

resource "aws_iam_user_policy" "sfe_upload" {
  name = "partner-inbound-sfe-upload"
  user = aws_iam_user.sfe_upload.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "UploadToSfePrefix"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = "${aws_s3_bucket.partner_inbound.arn}/sfe/*"
      },
      {
        Sid    = "ListSfePrefix"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
        ]
        Resource = aws_s3_bucket.partner_inbound.arn
        Condition = {
          StringLike = { "s3:prefix" = ["sfe/", "sfe/*"] }
        }
      },
      # GUI clients (Cyberduck/WinSCP) open the bucket root on connect; these two
      # statements let that root view work — folder names only (delimiter "/"
      # required, so a recursive no-delimiter dump of the bucket stays denied).
      # Two statements because clients variously send prefix="" or omit it, and
      # StringLike/StringEquals don't match an ABSENT context key.
      {
        Sid      = "ListRootFoldersEmptyPrefix"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.partner_inbound.arn
        Condition = {
          StringEquals = {
            "s3:prefix"    = ""
            "s3:delimiter" = "/"
          }
        }
      },
      {
        Sid      = "ListRootFoldersNoPrefix"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.partner_inbound.arn
        Condition = {
          Null         = { "s3:prefix" = "true" }
          StringEquals = { "s3:delimiter" = "/" }
        }
      },
      {
        Sid      = "BucketLocation" # Cyberduck/WinSCP resolve the region with this
        Effect   = "Allow"
        Action   = "s3:GetBucketLocation"
        Resource = aws_s3_bucket.partner_inbound.arn
      },
    ]
  })
}

output "partner_inbound_bucket" {
  description = "Inbound drop-zone bucket for external partner data deliveries"
  value       = aws_s3_bucket.partner_inbound.bucket
}
