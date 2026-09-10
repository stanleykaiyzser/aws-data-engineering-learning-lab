output "bucket_name" {
  value = aws_s3_bucket.lab.bucket
}

output "glue_job_name" {
  value = aws_glue_job.transform.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lab.name
}

