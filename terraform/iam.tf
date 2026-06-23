# --------------------------------------------------------------------------- #
# IAM: a role for the consumer Lambda and a role for Firehose.                   #
# --------------------------------------------------------------------------- #

# ------------------------------ Lambda role -------------------------------- #
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_inline" {
  statement {
    sid = "ReadEventStream"
    actions = [
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.events.arn]
  }
  statement {
    sid       = "DeliverToFirehose"
    actions   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
    resources = [aws_kinesis_firehose_delivery_stream.events.arn]
  }
  statement {
    sid       = "WriteAggregates"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/agg/*"]
  }
}

resource "aws_iam_role_policy" "lambda_inline" {
  name   = "${var.project}-lambda-inline"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_inline.json
}

# ----------------------------- Firehose role ------------------------------- #
data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.project}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose_inline" {
  statement {
    sid       = "WriteLake"
    actions   = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid     = "ReadGlueSchema"
    actions = ["glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions"]
    resources = [
      "arn:aws:glue:${var.region}:${local.account_id}:catalog",
      "arn:aws:glue:${var.region}:${local.account_id}:database/${aws_glue_catalog_database.stream.name}",
      "arn:aws:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.stream.name}/*",
    ]
  }
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["arn:aws:logs:${var.region}:${local.account_id}:log-group:/aws/kinesisfirehose/*"]
  }
}

resource "aws_iam_role_policy" "firehose_inline" {
  name   = "${var.project}-firehose-inline"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_inline.json
}
