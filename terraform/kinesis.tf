# --------------------------------------------------------------------------- #
# Kinesis Data Stream — the ingest buffer the producer writes to and the        #
# Lambda + Firehose read from. On-demand mode would also work; we pin a small   #
# shard count to stay predictable and cheap.                                    #
# --------------------------------------------------------------------------- #
resource "aws_kinesis_stream" "events" {
  name             = "${var.project}-events"
  shard_count      = var.shard_count
  retention_period = 24 # hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}
