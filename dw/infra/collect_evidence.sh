#!/usr/bin/env bash
# Genera las evidencias del DW en la EC2 y las descarga a dw/evidencias/ en tu computador.
# Uso (en tu computador, desde la carpeta dw):  ./infra/collect_evidence.sh
# La captura del tablero (evidencias/dashboard_quicksight.png) se conserva.
set -euo pipefail
source "$(dirname "$0")/common.sh"

DNS="$(live_dns)"
[ -n "$DNS" ] && [ "$DNS" != "None" ] || { echo "La EC2 no está encendida: (cd ../datalake && ./infra/ec2.sh start)" >&2; exit 1; }
BUCKET="$(dw_out bucket_name)"

echo ">> Generando evidencias del DW en la EC2 (Airflow, S3, Redshift; ~1 min)..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "ec2-user@${DNS}" \
  "cd ~/datalake && docker compose exec -T airflow-scheduler python -m dw.evidence"

echo ">> Descargando s3://${BUCKET}/evidence/ -> ${DW_ROOT}/evidencias/"
aws s3 sync "s3://${BUCKET}/evidence/" "${DW_ROOT}/evidencias/" --delete \
  --exclude "dashboard_quicksight.png" --exclude "*.png" --region "$AWS_DEFAULT_REGION"
echo ">> Listo. Abre dw/evidencias/README.md"
