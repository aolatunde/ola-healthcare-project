resource "aws_iam_role" "glue_service_role" {
  name = "${local.name_prefix}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_data_lake_policy" {
  name = "${local.name_prefix}-glue-data-lake-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:ListBucket"],
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "arn:aws:s3:::ola-credentials-bucket"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*",
          "arn:aws:s3:::ola-credentials-bucket/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:*",
          "cloudwatch:*",
          "glue:*"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_data_lake_attachment" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_data_lake_policy.arn
}

resource "aws_iam_role" "stepfunctions_role" {
  name = "${local.name_prefix}-stepfunctions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "states.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "stepfunctions_policy" {
  name = "${local.name_prefix}-stepfunctions-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ],
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = ["sns:Publish"],
        Resource = aws_sns_topic.pipeline_alerts.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "stepfunctions_policy_attachment" {
  role       = aws_iam_role.stepfunctions_role.name
  policy_arn = aws_iam_policy.stepfunctions_policy.arn
}

resource "aws_iam_role" "eventbridge_scheduler_role" {
  name = "${local.name_prefix}-eventbridge-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "scheduler.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "eventbridge_scheduler_policy" {
  name = "${local.name_prefix}-eventbridge-scheduler-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["states:StartExecution"],
      Resource = aws_sfn_state_machine.healthcare_pipeline.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_scheduler_policy_attachment" {
  role       = aws_iam_role.eventbridge_scheduler_role.name
  policy_arn = aws_iam_policy.eventbridge_scheduler_policy.arn
}