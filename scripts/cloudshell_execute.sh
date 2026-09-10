#!/usr/bin/env bash
set -euo pipefail

if [[ "${CONFIRM_AWS_COSTS:-}" != "YES" ]]; then
  echo "Execução bloqueada: use CONFIRM_AWS_COSTS=YES após aceitar o custo estimado de centavos de dólar." >&2
  exit 2
fi

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
region="${1:-us-east-1}"

if [[ -x "$project_dir/.tools/bin/terraform" ]]; then
  export PATH="$project_dir/.tools/bin:$PATH"
fi
if ! command -v terraform >/dev/null 2>&1; then
  echo "Terraform ausente. Execute primeiro: ./scripts/cloudshell_prepare.sh $region" >&2
  exit 3
fi
if [[ ! -f "$project_dir/terraform/tfplan" ]]; then
  echo "Plano ausente. Execute primeiro: ./scripts/cloudshell_prepare.sh $region" >&2
  exit 4
fi

mkdir -p "$project_dir/evidence"
terraform -chdir="$project_dir/terraform" apply tfplan
bucket="$(terraform -chdir="$project_dir/terraform" output -raw bucket_name)"

"$project_dir/scripts/run_aws.sh" "$bucket" 2025-01-01 2025-01-31 "$region" \
  | tee "$project_dir/evidence/glue_run.log"

python "$project_dir/src/run_athena.py" \
  --bucket "$bucket" \
  --region "$region" \
  --project-dir "$project_dir" \
  | tee "$project_dir/evidence/athena_run.log"

archive_path="$project_dir/../aws-data-engineering-learning-lab-executed.tar.gz"
tar \
  --exclude='.git' \
  --exclude='.venv' \
  --exclude='.tools' \
  --exclude='data' \
  --exclude='terraform/.terraform' \
  --exclude='terraform/terraform.tfstate*' \
  --exclude='terraform/terraform.tfvars' \
  --exclude='terraform/tfplan' \
  -czf "$archive_path" \
  -C "$project_dir/.." "$(basename "$project_dir")"

echo
echo "LAB AWS EXECUTADO COM SUCESSO"
echo "Recursos mantidos para estudo; não há compute permanente."
echo "Evidências: evidence/glue_run.log e evidence/athena_run.json"
echo "Pacote final: $archive_path"
echo "Quando terminar de estudar, use terraform -chdir=terraform destroy."
