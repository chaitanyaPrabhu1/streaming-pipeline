# --------------------------------------------------------------------------- #
# Kinesis Data Firehose: the consumer Lambda PUTs enriched JSON here; Firehose   #
# buffers it, converts JSON -> Parquet (using the Glue `events` table schema),   #
# and lands Hive-partitioned objects in s3://.../events/dt=.../hour=.../.        #
# --------------------------------------------------------------------------- #
resource "aws_kinesis_firehose_delivery_stream" "events" {
  name        = "${var.project}-events-delivery"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.lake.arn

    prefix              = "events/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "events_errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"

    buffering_size     = var.firehose_buffer_mb
    buffering_interval = var.firehose_buffer_seconds
    compression_format = "UNCOMPRESSED" # Parquet handles compression itself

    # JSON -> Parquet, using the Glue table as the schema source of truth.
    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
      schema_configuration {
        role_arn      = aws_iam_role.firehose.arn
        database_name = aws_glue_catalog_database.stream.name
        table_name    = aws_glue_catalog_table.events.name
        region        = var.region
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = "/aws/kinesisfirehose/${var.project}-events-delivery"
      log_stream_name = "S3Delivery"
    }
  }
}
