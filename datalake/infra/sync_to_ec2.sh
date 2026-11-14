#!/usr/bin/env bash
# Copia el código desde tu computador a la EC2 (sin GitHub).
# - No sobrescribe el .env de la EC2, ni copia llaves, estado de Terraform, logs o entornos virtuales.
# - Los cambios en dags/, src/ y config/ los toma Airflow sin reiniciar
#   (son volúmenes montados). Si cambias requirements.txt o Dockerfile, usa --rebuild.
#
# Uso:  ./infra/sync_to_ec2.sh            # solo copia
#       ./infra/sync_to_ec2.sh --rebuild  # copia, reconstruye la imagen y reinicia Airflow
set -euo pipefail
source "$(dirname "$0")/common.sh"

DNS="$(live_dns)"
if [ -z "$DNS" ] || [ "$DNS" = "None" ]; then
  echo "La EC2 no tiene DNS público. ¿Está encendida? ./infra/ec2.sh start" >&2
  exit 1
fi
SSH=(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "ec2-user@${DNS}")

echo ">> Copiando ${ROOT_DIR} -> ec2-user@${DNS}:~/datalake"
"${SSH[@]}" "mkdir -p ~/datalake/logs"
tar -C "$ROOT_DIR" \
  --exclude=./.env --exclude=./logs --exclude=./.venv --exclude=./.git \
  --exclude=./infra/terraform/.terraform --exclude='*.tfstate*' --exclude='*.tfvars' \
  --exclude='__pycache__' --exclude=.pytest_cache --exclude='*.zip' --exclude=./evidencias \
  -czf - . | "${SSH[@]}" "tar -xzf - -C ~/datalake"

if [ "${1:-}" = "--rebuild" ]; then
  echo ">> Reconstruyendo imagen y reiniciando Airflow"
  "${SSH[@]}" "cd ~/datalake && docker build -t rss-datalake-airflow:2.10.5 . && docker compose up -d --force-recreate"
fi

echo ">> Listo. Airflow: http://${DNS}:8080"
echo "   Nota: los archivos que borres localmente NO se borran en la EC2."
