resource "aws_cloudwatch_log_group" "runtime" {
  name              = "/blockchain/runtime/containers"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "blockchain-runtime-containers"
  }
}

resource "aws_sns_topic" "runtime_alerts" {
  name = "blockchain-runtime-alerts"
}

resource "aws_cloudwatch_metric_alarm" "instance_status" {
  alarm_name          = "blockchain-runtime-instance-status"
  alarm_description   = "EC2 system or instance status check failed"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 2
  metric_name         = "StatusCheckFailed"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Maximum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.runtime_alerts.arn]

  dimensions = {
    InstanceId = aws_instance.runtime.id
  }
}

resource "aws_cloudwatch_metric_alarm" "high_cpu" {
  alarm_name          = "blockchain-runtime-high-cpu"
  alarm_description   = "Sustained CPU may indicate T3 credit pressure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 4
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 70
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.runtime_alerts.arn]

  dimensions = {
    InstanceId = aws_instance.runtime.id
  }
}

data "aws_iam_policy_document" "events_publish_alerts" {
  statement {
    effect  = "Allow"
    actions = ["sns:Publish"]
    resources = [
      aws_sns_topic.runtime_alerts.arn,
    ]

    principals {
      type = "Service"
      identifiers = [
        "cloudwatch.amazonaws.com",
        "events.amazonaws.com",
      ]
    }
  }
}

resource "aws_sns_topic_policy" "runtime_alerts" {
  arn    = aws_sns_topic.runtime_alerts.arn
  policy = data.aws_iam_policy_document.events_publish_alerts.json
}

resource "aws_cloudwatch_event_rule" "runtime_state" {
  name        = "blockchain-runtime-state-change"
  description = "Notify when the controlled runtime stops or terminates"

  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Instance State-change Notification"]
    detail = {
      instance-id = [aws_instance.runtime.id]
      state       = ["stopped", "terminated"]
    }
  })
}

resource "aws_cloudwatch_event_target" "runtime_state" {
  rule = aws_cloudwatch_event_rule.runtime_state.name
  arn  = aws_sns_topic.runtime_alerts.arn
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduled_shutdown" {
  count = var.enable_scheduled_shutdown ? 1 : 0

  name               = "blockchain-runtime-scheduled-shutdown"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
}

data "aws_iam_policy_document" "scheduled_shutdown" {
  statement {
    effect    = "Allow"
    actions   = ["ec2:StopInstances"]
    resources = [aws_instance.runtime.arn]
  }
}

resource "aws_iam_role_policy" "scheduled_shutdown" {
  count = var.enable_scheduled_shutdown ? 1 : 0

  name   = "stop-blockchain-runtime"
  role   = aws_iam_role.scheduled_shutdown[0].id
  policy = data.aws_iam_policy_document.scheduled_shutdown.json
}

resource "aws_scheduler_schedule" "shutdown" {
  count = var.enable_scheduled_shutdown ? 1 : 0

  name                = "blockchain-runtime-stop"
  schedule_expression = var.shutdown_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"
    role_arn = aws_iam_role.scheduled_shutdown[0].arn
    input = jsonencode({
      InstanceIds = [aws_instance.runtime.id]
    })
  }
}
