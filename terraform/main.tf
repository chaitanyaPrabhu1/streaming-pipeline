data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id    = data.aws_caller_identity.current.account_id
  bucket_name   = "${var.project}-lake-${local.account_id}"
  glue_database = replace(var.project, "-", "_") # Glue/Athena dislike hyphens
}
