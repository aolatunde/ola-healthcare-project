variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "healthcare-delta"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "bucket_suffix" {
  type    = string
  default = "ola-001"
}

variable "alert_email" {
  type = string
}