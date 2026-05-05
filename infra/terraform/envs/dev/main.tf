locals {
  name_prefix = "${var.project_name}-${var.environment}"
  db_prefix   = replace(local.name_prefix, "-", "_")
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "${local.name_prefix}-data-lake-${var.bucket_suffix}"
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}