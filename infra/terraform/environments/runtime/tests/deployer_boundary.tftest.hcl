# Credential-free provider mocks. No backend, IAM writes, or EC2 launch.
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
  docker_compose_download_url = "https://github.com/docker/compose/releases/download/v5.1.3/docker-compose-linux-x86_64"
  docker_compose_sha256       = "a0298760c9772d2c06888fc8703a487c94c3c3b0134adeef830742a2fc7647b4"
}

run "foundation_default_preserved" {
  command = plan

  assert {
    condition     = aws_iam_role.runtime.permissions_boundary == null
    error_message = "The existing foundation must not silently gain a boundary."
  }
}

run "exact_administrator_boundary" {
  command = plan

  variables {
    runtime_permissions_boundary_arn = "arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary"
  }

  assert {
    condition     = aws_iam_role.runtime.permissions_boundary == var.runtime_permissions_boundary_arn
    error_message = "The runtime EC2 role must carry the exact administrator-owned boundary."
  }
}

run "other_boundary_rejected" {
  command = plan

  variables {
    runtime_permissions_boundary_arn = "arn:aws:iam::209479274979:policy/unreviewed-boundary"
  }
  expect_failures = [var.runtime_permissions_boundary_arn]
}

run "secret_opt_in_requires_new_boundary_review" {
  command = plan

  variables {
    runtime_permissions_boundary_arn = "arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary"
    approved_secret_arns             = ["arn:aws:secretsmanager:us-west-2:209479274979:secret:offline-test-alchemy-ABC123"]
  }
  expect_failures = [aws_iam_role.runtime]
}
