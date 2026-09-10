variable "aws_region" {
  description = "Region used by the disposable learning lab."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name, e.g. weather-learning-<account-id>."
  type        = string
}

