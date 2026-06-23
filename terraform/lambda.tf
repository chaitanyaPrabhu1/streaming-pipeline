# --------------------------------------------------------------------------- #
# Consumer Lambda + the Kinesis event-source mapping that drives it. The deploy  #
# package is assembled by `make package` into build/package (run before apply).  #
# --------------------------------------------------------------------------- #
data "archive_file" "consumer" {
  type        = "zip"
  source_dir  = "${path.module}/../build/package"
  output_path = "${path.module}/build/consumer.zip"
}

resource "aws_lambda_function" "consumer" {
  function_name    = "${var.project}-consumer"
  role             = aws_iam_role.lambda.arn
  handler          = "consumer.handler"
  runtime          = "python3.11"
  filename         = data.archive_file.consumer.output_path
  source_code_hash = data.archive_file.consumer.output_base64sha256
  timeout          = 60
  memory_size      = 256

  # AWS-managed pandas/pyarrow layer (provides pyarrow for the agg Parquet write).
  layers = [var.pandas_layer_arn]

  environment {
    variables = {
      DATA_BUCKET     = aws_s3_bucket.lake.bucket
      FIREHOSE_STREAM = aws_kinesis_firehose_delivery_stream.events.name
      WINDOW_SECONDS  = "60"
    }
  }
}

resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn  = aws_kinesis_stream.events.arn
  function_name     = aws_lambda_function.consumer.arn
  starting_position = "LATEST"

  batch_size                         = var.lambda_batch_size
  maximum_batching_window_in_seconds = var.lambda_batch_window_seconds
  parallelization_factor             = 1

  # Resilience: don't let one poison record block the shard forever.
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 5
}
