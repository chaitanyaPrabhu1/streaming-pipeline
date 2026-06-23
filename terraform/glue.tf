# --------------------------------------------------------------------------- #
# Glue Data Catalog: a database + two Parquet tables with partition projection  #
# (no crawler needed). The `events` table doubles as Firehose's format-          #
# conversion schema. Athena queries both; the rollup view lives in sql/.         #
# --------------------------------------------------------------------------- #
resource "aws_glue_catalog_database" "stream" {
  name = local.glue_database
}

locals {
  events_location = "s3://${aws_s3_bucket.lake.bucket}/events/"
  agg_location    = "s3://${aws_s3_bucket.lake.bucket}/agg/"

  # Partition projection shared by both tables (dt + hour from the S3 path).
  projection_params = {
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "2024-01-01,NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.hour.type"        = "integer"
    "projection.hour.range"       = "0,23"
    "projection.hour.digits"      = "2"
    "classification"              = "parquet"
    "EXTERNAL"                    = "TRUE"
  }
}

resource "aws_glue_catalog_table" "events" {
  name          = "events"
  database_name = aws_glue_catalog_database.stream.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(local.projection_params, {
    "storage.location.template" = "${local.events_location}dt=$${dt}/hour=$${hour}/"
  })

  partition_keys {
    name = "dt"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }

  storage_descriptor {
    location      = local.events_location
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns { name = "event_id"      type = "string" }
    columns { name = "event_time"    type = "timestamp" }
    columns { name = "event_type"    type = "string" }
    columns { name = "wiki"          type = "string" }
    columns { name = "domain"        type = "string" }
    columns { name = "title"         type = "string" }
    columns { name = "user"          type = "string" }
    columns { name = "is_bot"        type = "boolean" }
    columns { name = "is_minor"      type = "boolean" }
    columns { name = "namespace"     type = "bigint" }
    columns { name = "length_old"    type = "bigint" }
    columns { name = "length_new"    type = "bigint" }
    columns { name = "bytes_changed" type = "bigint" }
    columns { name = "ingested_at"   type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "agg_partials" {
  name          = "agg_partials"
  database_name = aws_glue_catalog_database.stream.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(local.projection_params, {
    "storage.location.template" = "${local.agg_location}dt=$${dt}/hour=$${hour}/"
  })

  partition_keys {
    name = "dt"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }

  storage_descriptor {
    location      = local.agg_location
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns { name = "window_start"        type = "timestamp" }
    columns { name = "wiki"                type = "string" }
    columns { name = "event_count"         type = "bigint" }
    columns { name = "edit_count"          type = "bigint" }
    columns { name = "new_page_count"      type = "bigint" }
    columns { name = "bot_count"           type = "bigint" }
    columns { name = "total_bytes_changed" type = "bigint" }
    columns { name = "abs_bytes_changed"   type = "bigint" }
    columns { name = "batch_id"            type = "string" }
    columns { name = "processed_at"        type = "timestamp" }
  }
}

resource "aws_athena_workgroup" "stream" {
  name          = local.glue_database
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
