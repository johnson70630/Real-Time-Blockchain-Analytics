output "production_data_bucket_name" {
  description = "Name of the adopted production analytical-data bucket."
  value       = aws_s3_bucket.production_data.id
}

output "snowflake_s3_role_arn" {
  description = "ARN of the adopted IAM role used by the Snowflake storage integration."
  value       = aws_iam_role.snowflake_s3.arn
}
