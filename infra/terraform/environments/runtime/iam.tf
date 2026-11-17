locals {
  # This is a review artifact, not a Terraform-managed boundary. Its live
  # default version must be read back separately before supplying the receipt.
  runtime_secret_boundary_candidate = jsondecode(file("${path.module}/policies/runtime-secret-boundary.proposed.json"))
  runtime_secret_boundary_sha256    = sha256(jsonencode(local.runtime_secret_boundary_candidate))
  runtime_secret_boundary_arns = one([
    for statement in local.runtime_secret_boundary_candidate.Statement : statement.Resource
    if statement.Sid == "ReadOnlyPilotAlchemySecret"
  ])
}

data "aws_iam_policy_document" "runtime_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "runtime" {
  name                 = "blockchain-runtime-ec2"
  assume_role_policy   = data.aws_iam_policy_document.runtime_assume_role.json
  max_session_duration = 3600
  permissions_boundary = var.runtime_permissions_boundary_arn

  lifecycle {
    precondition {
      condition = (
        var.runtime_permissions_boundary_arn == null ||
        nonsensitive(length(var.approved_secret_arns)) == 0 ||
        (
          var.runtime_secret_boundary_review_sha256 == local.runtime_secret_boundary_sha256 &&
          toset(var.approved_secret_arns) == toset(local.runtime_secret_boundary_arns)
        )
      )
      error_message = "Secret access under the demo boundary requires the exact reviewed secret and a matching canonical SHA-256 receipt from its live default-policy readback."
    }
    precondition {
      condition = (
        var.runtime_secret_boundary_review_sha256 == null ||
        (
          var.runtime_permissions_boundary_arn != null &&
          var.runtime_secret_boundary_review_sha256 == local.runtime_secret_boundary_sha256 &&
          toset(var.approved_secret_arns) == toset(local.runtime_secret_boundary_arns)
        )
      )
      error_message = "A boundary-review receipt is valid only for the selected demo boundary and exact reviewed secret; stale or mismatched receipts are forbidden."
    }
  }
}

resource "aws_iam_instance_profile" "runtime" {
  name = "blockchain-runtime-ec2"
  role = aws_iam_role.runtime.name
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.runtime.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "runtime" {
  statement {
    sid       = "GetPilotBucketLocation"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.data_bucket_name}"]
  }

  statement {
    sid       = "ListOnlyPilotData"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.data_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        var.pilot_s3_prefix,
        "${var.pilot_s3_prefix}/*",
      ]
    }
  }

  statement {
    sid       = "ListPilotMultipartUploads"
    effect    = "Allow"
    actions   = ["s3:ListBucketMultipartUploads"]
    resources = ["arn:aws:s3:::${var.data_bucket_name}"]
  }

  statement {
    sid    = "ReadWriteOnlyPilotData"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = [
      "arn:aws:s3:::${var.data_bucket_name}/${var.pilot_s3_prefix}/*",
    ]
  }

  statement {
    sid    = "DenyTerraformStateBucketMetadata"
    effect = "Deny"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketVersions",
    ]
    resources = ["arn:aws:s3:::${var.terraform_state_bucket_name}"]
  }

  statement {
    sid    = "DenyTerraformStateObjects"
    effect = "Deny"
    actions = [
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
    ]
    resources = ["arn:aws:s3:::${var.terraform_state_bucket_name}/*"]
  }

  dynamic "statement" {
    for_each = nonsensitive(length(var.approved_secret_arns) > 0) ? [true] : []
    content {
      sid       = "ReadApprovedRuntimeSecrets"
      effect    = "Allow"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = var.approved_secret_arns

      condition {
        test     = "StringEquals"
        variable = "aws:RequestedRegion"
        values   = ["us-west-2"]
      }
    }
  }

  statement {
    sid       = "GetEcrAuthorizationToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "PullApprovedRuntimeImages"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [
      aws_ecr_repository.producer.arn,
      aws_ecr_repository.spark.arn,
    ]
  }

  statement {
    sid    = "WriteRuntimeLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.runtime.arn}:*"]
  }
}

resource "aws_iam_role_policy" "runtime" {
  name   = "blockchain-runtime-least-privilege"
  role   = aws_iam_role.runtime.id
  policy = data.aws_iam_policy_document.runtime.json
}
