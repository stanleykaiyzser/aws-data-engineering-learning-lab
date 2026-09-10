#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
region="${1:-us-east-1}"
terraform_version="1.16.2"

account_id="$(aws sts get-caller-identity --query Account --output text)"
account_hash="$(printf '%s' "$account_id" | sha256sum | cut -c1-10)"
echo "Sessão AWS autenticada confirmada."

case "$(uname -m)" in
  x86_64) terraform_arch="amd64" ;;
  aarch64|arm64) terraform_arch="arm64" ;;
  *) echo "Arquitetura não suportada: $(uname -m)" >&2; exit 2 ;;
esac

if ! command -v terraform >/dev/null 2>&1; then
  tools_dir="$project_dir/.tools/bin"
  download_dir="$(mktemp -d)"
  archive="terraform_${terraform_version}_linux_${terraform_arch}.zip"
  mkdir -p "$tools_dir"
  curl -fsSLo "$download_dir/$archive" "https://releases.hashicorp.com/terraform/${terraform_version}/$archive"
  curl -fsSLo "$download_dir/SHA256SUMS" "https://releases.hashicorp.com/terraform/${terraform_version}/terraform_${terraform_version}_SHA256SUMS"
  (
    cd "$download_dir"
    grep " $archive\$" SHA256SUMS | sha256sum --check -
    unzip -oq "$archive" -d "$tools_dir"
  )
  export PATH="$tools_dir:$PATH"
fi

bucket="aws-de-learning-lab-${account_hash}-${region//-/}"
printf 'bucket_name = "%s"\naws_region  = "%s"\n' "$bucket" "$region" \
  > "$project_dir/terraform/terraform.tfvars"

python -m pip install -q -r "$project_dir/requirements.txt"

cd "$project_dir/terraform"
terraform init
terraform fmt -check
terraform validate
terraform plan -out=tfplan

echo
echo "PREPARAÇÃO CONCLUÍDA — nenhum recurso foi criado."
echo "Bucket planejado: $bucket"
echo "Plano salvo em: terraform/tfplan"
echo "Para executar tudo após autorizar o pequeno custo:"
echo "CONFIRM_AWS_COSTS=YES ./scripts/cloudshell_execute.sh $region"
