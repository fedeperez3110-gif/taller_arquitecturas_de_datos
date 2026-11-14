#!/usr/bin/env bash
# Copia dw/ y datalake/ desde tu computador a la EC2 (sin GitHub).
# No copia .env, llaves, estado de Terraform ni archivos generados por dbt.
#
# Uso:  ./infra/sync_to_ec2.sh            # solo copia (Airflow toma los cambios solo)
#       ./infra/sync_to_ec2.sh --rebuild  # además reconstruye la imagen y reinicia Airflow
#                                         # (necesario la primera vez: dbt, pymysql, certificado)
set -euo pipefail
source "$(dirname "$0")/common.sh"

DNS="$(live_dns)"
if [ -z "$DNS" ] || [ "$DNS" = "None" ]; then
  echo "La EC2 no tiene DNS público. ¿Está encendida? (cd ../datalake && ./infra/ec2.sh start)" >&2
  exit 1
fi
SSH=(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "ec2-user@${DNS}")
EXCLUDES=(--exclude=./.env --exclude=./.git --exclude=./.venv --exclude='__pycache__' --exclude=.pytest_cache
          --exclude=./infra/terraform/.terraform --exclude='*.tfstate*' --exclude='*.tfvars' --exclude='*.zip'
          --exclude=./dbt/target --exclude=./dbt/dbt_packages --exclude=./dbt/logs --exclude=./evidencias)

echo ">> dw -> ec2-user@${DNS}:~/dw"
"${SSH[@]}" "mkdir -p ~/dw"
tar -C "$DW_ROOT" "${EXCLUDES[@]}" -czf - . | "${SSH[@]}" "tar -xzf - -C ~/dw"

echo ">> datalake (Dockerfile, requirements y docker-compose con los cambios del DW) -> ~/datalake"
tar -C "$DATALAKE_ROOT" --exclude=./.env --exclude=./logs --exclude=./.venv --exclude=./.git \
  --exclude=./infra/terraform/.terraform --exclude='*.tfstate*' --exclude='*.tfvars' \
  --exclude='__pycache__' --exclude=.pytest_cache --exclude='*.zip' --exclude=./evidencias --exclude=./dags/dw \
  -czf - . | "${SSH[@]}" "tar -xzf - -C ~/datalake"

if [ "${1:-}" = "--rebuild" ]; then
  echo ">> Reconstruyendo la imagen (incluye dbt) y reiniciando Airflow"
  "${SSH[@]}" "cd ~/datalake && docker build -t rss-datalake-airflow:2.10.5 . && docker compose up -d --force-recreate"
fi
echo ">> Listo. Airflow: http://${DNS}:8080"
