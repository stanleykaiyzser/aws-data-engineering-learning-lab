#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Uso: $0 BUCKET [START_DATE] [END_DATE] [REGION]" >&2
  exit 2
fi

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bucket="$1"
start_date="${2:-2025-01-01}"
end_date="${3:-2025-01-31}"
region="${4:-us-east-1}"

cd "$project_dir"
aws sts get-caller-identity >/dev/null
aws s3 cp glue/weather_transform.py "s3://$bucket/code/weather_transform.py" --region "$region"
python -m src.ingest --start-date "$start_date" --end-date "$end_date" --bucket "$bucket"

run_id="$(aws glue start-job-run \
  --job-name weather-learning-transform \
  --region "$region" \
  --arguments "{\"--RAW_PATH\":\"s3://$bucket/raw/open_meteo/\",\"--CURATED_PATH\":\"s3://$bucket/curated/\",\"--QUALITY_PATH\":\"s3://$bucket/quality/latest/\"}" \
  --query JobRunId --output text)"
echo "Glue JobRunId: $run_id"

while true; do
  state="$(aws glue get-job-run --job-name weather-learning-transform --run-id "$run_id" --region "$region" --query JobRun.JobRunState --output text)"
  echo "Glue: $state"
  case "$state" in
    SUCCEEDED) break ;;
    FAILED|STOPPED|TIMEOUT|ERROR|EXPIRED)
      aws glue get-job-run --job-name weather-learning-transform --run-id "$run_id" --region "$region" --query 'JobRun.ErrorMessage' --output text
      exit 1
      ;;
  esac
  sleep 15
done

echo "Pipeline AWS concluído. Agora execute sql/01_catalog.sql no Athena substituindo \${BUCKET} por $bucket."

