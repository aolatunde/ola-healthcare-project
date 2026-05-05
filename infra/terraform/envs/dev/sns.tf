resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}