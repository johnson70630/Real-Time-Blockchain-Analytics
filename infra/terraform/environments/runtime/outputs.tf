output "runtime_instance_id" {
  description = "EC2 runtime instance identifier."
  value       = aws_instance.runtime.id
}

output "kafka_volume_id" {
  description = "Persistent Kafka EBS volume identifier."
  value       = aws_ebs_volume.kafka.id
}

output "runtime_log_group" {
  description = "CloudWatch log group for container logs."
  value       = aws_cloudwatch_log_group.runtime.name
}

output "producer_repository_url" {
  description = "ECR repository for immutable producer images."
  value       = aws_ecr_repository.producer.repository_url
}

output "spark_repository_url" {
  description = "ECR repository for immutable Spark images."
  value       = aws_ecr_repository.spark.repository_url
}

output "pilot_data_uri" {
  description = "Isolated pilot data root; production Bronze is out of scope."
  value       = "s3://${var.data_bucket_name}/${var.pilot_s3_prefix}/bronze/"
}

output "pilot_checkpoint_uri" {
  description = "Single-writer checkpoint root for the first pilot."
  value       = "s3://${var.data_bucket_name}/${var.pilot_s3_prefix}/checkpoints/"
}
