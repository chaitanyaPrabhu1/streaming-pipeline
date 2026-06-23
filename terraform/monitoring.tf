# --------------------------------------------------------------------------- #
# Guardrails: billing + the two alarms that matter for a stream — Lambda errors  #
# and consumer LAG (iterator age = how far behind real time we are). All wired   #
# to email via SNS, skipped entirely if alarm_email is empty.                    #
# --------------------------------------------------------------------------- #
resource "aws_sns_topic" "alerts" {
  count = var.alarm_email == "" ? 0 : 1
  name  = "${var.project}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "billing" {
  count               = var.alarm_email == "" ? 0 : 1
  alarm_name          = "${var.project}-estimated-charges"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = 21600 # 6h
  statistic           = "Maximum"
  threshold           = var.monthly_cost_alarm_usd
  alarm_description   = "Estimated AWS charges exceeded $${var.monthly_cost_alarm_usd}."
  dimensions          = { Currency = "USD" }
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
}

resource "aws_cloudwatch_metric_alarm" "consumer_errors" {
  count               = var.alarm_email == "" ? 0 : 1
  alarm_name          = "${var.project}-consumer-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
}

# Iterator age = the consumer's lag behind the tip of the stream. The single
# most important health metric for a streaming consumer.
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  count               = var.alarm_email == "" ? 0 : 1
  alarm_name          = "${var.project}-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "IteratorAge"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Maximum"
  threshold           = 60000 # 60s behind real time
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  alarm_description   = "Consumer is falling behind the stream (>60s)."
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
}
