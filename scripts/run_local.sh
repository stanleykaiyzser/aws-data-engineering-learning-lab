#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data_dir="${1:-$project_dir/data}"
start_date="${2:-2025-01-01}"
end_date="${3:-2025-01-07}"

cd "$project_dir"
if ! command -v spark-submit >/dev/null 2>&1; then
  echo "spark-submit não encontrado. Ative o ambiente com: source .venv/bin/activate" >&2
  exit 127
fi
python -m src.ingest --start-date "$start_date" --end-date "$end_date" --local-dir "$data_dir"
spark-submit glue/weather_transform.py \
  --RAW_PATH "$data_dir/raw/open_meteo" \
  --CURATED_PATH "$data_dir/curated" \
  --QUALITY_PATH "$data_dir/quality/latest"
