#!/usr/bin/env bash
# Genera las evidencias en la EC2 y las descarga a ./evidencias/ en tu computador.
# Uso (en tu computador, desde la carpeta del proyecto):  ./infra/collect_evidence.sh
set -euo pipefail
source "$(dirname "$0")/common.sh"

DNS="$(live_dns)"
[ -n "$DNS" ] && [ "$DNS" != "None" ] || { echo "La EC2 no está encendida: ./infra/ec2.sh start" >&2; exit 1; }
BUCKET="$(tf_out bucket_name)"

echo ">> Generando evidencias en la EC2 (consulta Airflow, S3, Glue y Athena; tarda ~1-2 min)..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "ec2-user@${DNS}" \
  "cd ~/datalake && docker compose exec -T airflow-scheduler python -m datalake.evidence"

echo ">> Descargando s3://${BUCKET}/evidence/ -> ${ROOT_DIR}/evidencias/"
aws s3 sync "s3://${BUCKET}/evidence/" "${ROOT_DIR}/evidencias/" --delete --region "$AWS_DEFAULT_REGION"
echo ">> Listo. Abre evidencias/README.md"
