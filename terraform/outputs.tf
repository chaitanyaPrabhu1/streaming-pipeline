output "data_bucket" {
  description = "S3 data-lake bucket. Export as DATA_BUCKET."
  value       = aws_s3_bucket.lake.bucket
}

output "stream_name" {
  description = "Kinesis data stream. Export as STREAM_NAME for the producer."
  value       = aws_kinesis_stream.events.name
}

output "firehose_stream" {
  description = "Firehose delivery stream. Export as FIREHOSE_STREAM."
  value       = aws_kinesis_firehose_delivery_stream.events.name
}

output "consumer_lambda" {
  description = "Consumer Lambda name."
  value       = aws_lambda_function.consumer.function_name
}

output "glue_database" {
  description = "Glue/Athena database. Export as GLUE_DATABASE."
  value       = aws_glue_catalog_database.stream.name
}

output "athena_workgroup" {
  description = "Athena workgroup. Export as ATHENA_WORKGROUP."
  value       = aws_athena_workgroup.stream.name
}

output "env_exports" {
  description = "Copy-paste block to run the producer + query Athena."
  value       = <<-EOT
    export DATA_BUCKET=${aws_s3_bucket.lake.bucket}
    export AWS_REGION=${var.region}
    export STREAM_NAME=${aws_kinesis_stream.events.name}
    export FIREHOSE_STREAM=${aws_kinesis_firehose_delivery_stream.events.name}
    export GLUE_DATABASE=${aws_glue_catalog_database.stream.name}
    export ATHENA_WORKGROUP=${aws_athena_workgroup.stream.name}
  EOT
}
