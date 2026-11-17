# Mock-only plans. The receipt is never evidence of a live readback in this test.
mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
  mock_data "aws_ssm_parameter" {
    defaults = {
      value = "ami-0123456789abcdef0"
    }
  }
}

variables {
  docker_compose_download_url           = "https://github.com/docker/compose/releases/download/v5.1.3/docker-compose-linux-x86_64"
  docker_compose_sha256                 = "a0298760c9772d2c06888fc8703a487c94c3c3b0134adeef830742a2fc7647b4"
  runtime_permissions_boundary_arn      = "arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary"
  approved_secret_arns                  = ["arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr"]
  runtime_secret_boundary_review_sha256 = "1ae18de848a97a218592fda0f1fb392002cd3ede44beb04e4ce0d3eedccb7e2a"
}

run "exact_secret_and_verified_boundary_receipt" {
  command = plan

  assert {
    condition     = local.runtime_secret_boundary_sha256 == var.runtime_secret_boundary_review_sha256
    error_message = "The canonical document hash must match the independently reviewed snapshot."
  }
  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.runtime.statement : statement
      if statement.sid == "ReadApprovedRuntimeSecrets" &&
      statement.resources == toset(var.approved_secret_arns) &&
      statement.actions == toset(["secretsmanager:GetSecretValue"]) &&
      length([for c in statement.condition : c if c.test == "StringEquals" && c.variable == "aws:RequestedRegion" && toset(c.values) == toset(["us-west-2"])]) == 1
    ]) == 1
    error_message = "The runtime grant must remain one regional GetSecretValue statement on the exact ARN."
  }
}

run "missing_receipt_rejected" {
  command = plan
  variables {
    runtime_secret_boundary_review_sha256 = null
  }
  expect_failures = [aws_iam_role.runtime]
}

run "mismatched_receipt_rejected" {
  command = plan
  variables {
    runtime_secret_boundary_review_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
  }
  expect_failures = [aws_iam_role.runtime]
}

run "different_secret_rejected_with_receipt" {
  command = plan
  variables {
    approved_secret_arns = ["arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-ABC123"]
  }
  expect_failures = [aws_iam_role.runtime]
}

run "additional_secret_rejected_with_receipt" {
  command = plan
  variables {
    approved_secret_arns = [
      "arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr",
      "arn:aws:secretsmanager:us-west-2:209479274979:secret:other-secret-ABC123",
    ]
  }
  expect_failures = [aws_iam_role.runtime]
}

run "receipt_without_secret_rejected" {
  command = plan
  variables {
    approved_secret_arns = []
  }
  expect_failures = [aws_iam_role.runtime]
}

run "receipt_without_boundary_rejected" {
  command = plan
  variables {
    runtime_permissions_boundary_arn = null
  }
  expect_failures = [aws_iam_role.runtime]
}
