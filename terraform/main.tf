terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = "aws-data-engineering-learning-lab"
      Purpose = "learning"
    }
  }
}

resource "aws_s3_bucket" "lab" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lab" {
  bucket                  = aws_s3_bucket.lab.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id

  rule {
    id     = "expire-temporary-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

resource "aws_glue_catalog_database" "lab" {
  name = "weather_learning_lab"
}

resource "aws_athena_workgroup" "lab" {
  name          = "weather-learning-lab"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = 104857600 # 100 MiB safety rail
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lab.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "weather-learning-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue_lab" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lab.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lab.arn}/*"]
  }
  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:GetPartitions", "glue:BatchCreatePartition"
    ]
    resources = ["*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_role_policy" "glue_lab" {
  name   = "weather-learning-glue-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_lab.json
}

resource "aws_glue_job" "transform" {
  name              = "weather-learning-transform"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lab.bucket}/code/weather_transform.py"
  }

  default_arguments = {
    "--RAW_PATH"                         = "s3://${aws_s3_bucket.lab.bucket}/raw/open_meteo/"
    "--CURATED_PATH"                     = "s3://${aws_s3_bucket.lab.bucket}/curated/"
    "--QUALITY_PATH"                     = "s3://${aws_s3_bucket.lab.bucket}/quality/latest/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }
}
