output "data_lake_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue_service_role.arn
}

output "stepfunctions_state_machine_arn" {
  value = aws_sfn_state_machine.healthcare_pipeline.arn
}

output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "eventbridge_schedule_name" {
  value = aws_scheduler_schedule.weekly_pipeline.name
}