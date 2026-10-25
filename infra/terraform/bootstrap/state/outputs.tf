output "state_bucket_name" {
  description = "Bucket name to place in the production backend configuration."
  value       = aws_s3_bucket.terraform_state.id
}

output "state_bucket_region" {
  description = "Region of the dedicated Terraform state bucket."
  value       = var.aws_region
}
