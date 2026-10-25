locals {
  production_data_bucket_name = "johnson-blockchain-analytics-prod"
  snowflake_s3_policy_name    = "snowflake-blockchain-s3-read"
  snowflake_s3_role_name      = "snowflake-blockchain-s3-role"
}

resource "aws_s3_bucket" "production_data" {
  provider = aws.adopted

  bucket        = local.production_data_bucket_name
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "production_data" {
  provider = aws.adopted

  bucket = aws_s3_bucket.production_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "production_data" {
  provider = aws.adopted

  bucket = aws_s3_bucket.production_data.id

  rule {
    blocked_encryption_types = ["SSE-C"]
    bucket_key_enabled       = true

    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "production_data" {
  provider = aws.adopted

  bucket = aws_s3_bucket.production_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "production_data" {
  provider = aws.adopted

  bucket = aws_s3_bucket.production_data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

data "aws_iam_policy_document" "snowflake_s3_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::189624869107:user/hmza2000-s"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_storage_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_s3" {
  provider = aws.adopted

  name                 = local.snowflake_s3_role_name
  path                 = "/"
  assume_role_policy   = data.aws_iam_policy_document.snowflake_s3_assume_role.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "snowflake_s3_read" {
  statement {
    sid    = "ReadBlockchainProductionObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "${aws_s3_bucket.production_data.arn}/blockchain-analytics/production/*",
    ]
  }

  statement {
    sid    = "ListBlockchainProductionPrefix"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.production_data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["blockchain-analytics/production/*"]
    }
  }
}

resource "aws_iam_policy" "snowflake_s3_read" {
  provider = aws.adopted

  name   = local.snowflake_s3_policy_name
  path   = "/"
  policy = data.aws_iam_policy_document.snowflake_s3_read.json
}

resource "aws_iam_role_policy_attachment" "snowflake_s3_read" {
  provider = aws.adopted

  role       = aws_iam_role.snowflake_s3.name
  policy_arn = aws_iam_policy.snowflake_s3_read.arn
}
