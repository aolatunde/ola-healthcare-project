resource "aws_sfn_state_machine" "healthcare_pipeline" {
  name     = "${local.name_prefix}-pipeline"
  role_arn = aws_iam_role.stepfunctions_role.arn

  definition = templatefile("${path.module}/state_machine.asl.json", {
    sns_topic_arn = aws_sns_topic.pipeline_alerts.arn
  })
}