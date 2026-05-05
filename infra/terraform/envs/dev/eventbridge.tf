resource "aws_scheduler_schedule" "weekly_pipeline" {
  name       = "${local.name_prefix}-weekly-pipeline-schedule"
  group_name = "default"

  schedule_expression = "cron(0 8 ? * MON *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.healthcare_pipeline.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    input = jsonencode({
      source      = "eventbridge-scheduler"
      pipeline    = local.name_prefix
      environment = var.environment
    })
  }
}