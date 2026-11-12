# Offline provider mocks: no AWS credentials, backend access, or resources.
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

run "no_secrets_by_default" {
  command = plan

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.runtime.statement : statement
      if statement.sid == "ReadApprovedRuntimeSecrets"
    ]) == 0
    error_message = "Infrastructure-only planning must omit all secret-read statements."
  }
}

run "exact_secret_opt_in" {
  command = plan

  variables {
    approved_secret_arns = ["arn:aws:secretsmanager:us-west-2:209479274979:secret:offline-test-alchemy-ABC123"]
  }

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.runtime.statement : statement
      if statement.sid == "ReadApprovedRuntimeSecrets" &&
      statement.resources == toset(var.approved_secret_arns) &&
      statement.actions == toset(["secretsmanager:GetSecretValue"])
    ]) == 1
    error_message = "Opt-in access must name only the exact supplied secret ARNs."
  }
}

run "wildcard_secret_rejected" {
  command = plan

  variables {
    approved_secret_arns = ["arn:aws:secretsmanager:us-west-2:209479274979:secret:*"]
  }
  expect_failures = [var.approved_secret_arns]
}

run "different_account_rejected" {
  command = plan

  variables {
    approved_secret_arns = ["arn:aws:secretsmanager:us-west-2:000000000000:secret:offline-test-ABC123"]
  }
  expect_failures = [var.approved_secret_arns]
}

run "different_region_rejected" {
  command = plan

  variables {
    approved_secret_arns = ["arn:aws:secretsmanager:us-east-1:209479274979:secret:offline-test-ABC123"]
  }
  expect_failures = [var.approved_secret_arns]
}
