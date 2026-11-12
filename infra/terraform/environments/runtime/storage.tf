resource "aws_ebs_volume" "kafka" {
  availability_zone = var.availability_zone
  size              = var.kafka_volume_size_gib
  type              = "gp3"
  encrypted         = true
  iops              = 3000
  throughput        = 125

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name      = "blockchain-runtime-kafka"
    State     = "persistent"
    Retention = "72-hours"
  }
}
