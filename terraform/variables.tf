variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "wiki-stream"
}

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "shard_count" {
  description = "Kinesis shard count (1 handles the Wikimedia firehose comfortably)."
  type        = number
  default     = 1
}

variable "lambda_batch_size" {
  description = "Max records per Lambda invocation from the Kinesis ESM."
  type        = number
  default     = 200
}

variable "lambda_batch_window_seconds" {
  description = "Max seconds the ESM buffers before invoking (time-based batching)."
  type        = number
  default     = 30
}

variable "firehose_buffer_seconds" {
  description = "Firehose buffering interval before flushing a Parquet object to S3."
  type        = number
  default     = 60
}

variable "firehose_buffer_mb" {
  description = "Firehose buffering size (MB) before flushing to S3."
  type        = number
  default     = 64
}

variable "pandas_layer_arn" {
  description = <<-EOT
    ARN of the AWS-managed "AWSSDKPandas" Lambda layer for this region/runtime
    (provides pandas + pyarrow). Find the current ARN for python3.11 here:
    https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
    Example (us-east-1):
    arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:13
  EOT
  type        = string
}

variable "events_expiration_days" {
  description = "Days before landed event objects expire (keeps the lake cheap)."
  type        = number
  default     = 30
}

variable "alarm_email" {
  description = "Email to receive billing + pipeline alarms (leave empty to skip)."
  type        = string
  default     = ""
}

variable "monthly_cost_alarm_usd" {
  description = "Estimated-charges threshold (USD) that triggers the billing alarm."
  type        = number
  default     = 5
}
