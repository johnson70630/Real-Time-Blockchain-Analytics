data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_instance" "runtime" {
  ami                         = data.aws_ssm_parameter.al2023_ami.value
  instance_type               = var.instance_type
  availability_zone           = var.availability_zone
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.runtime.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.runtime.name

  disable_api_termination              = true
  instance_initiated_shutdown_behavior = "stop"
  monitoring                           = false

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "disabled"
  }

  credit_specification {
    cpu_credits = "standard"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gib
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/templates/user_data.sh.tftpl", {
    docker_compose_download_url = var.docker_compose_download_url
    docker_compose_sha256       = var.docker_compose_sha256
    kafka_volume_id             = aws_ebs_volume.kafka.id
    kafka_mount_point           = "/var/lib/blockchain-runtime/kafka"
  })

  # Bootstrap is intentionally inert: it mounts storage and installs the
  # runtime, but it does not fetch secrets or start application containers.
  user_data_replace_on_change = false

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name          = "blockchain-runtime"
    RuntimePolicy = "operator-controlled"
  }
}

resource "aws_volume_attachment" "kafka" {
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.kafka.id
  instance_id                    = aws_instance.runtime.id
  stop_instance_before_detaching = true
}
