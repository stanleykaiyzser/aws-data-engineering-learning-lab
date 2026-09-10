#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo "Uso: $0 BUCKET" >&2
  exit 2
fi
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sed "s/\${BUCKET}/$1/g" "$project_dir/sql/01_catalog.sql"

