#!/usr/bin/env bash
# Funciones compartidas del DW. La EC2 de Airflow es la del proyecto datalake (carpeta hermana).
DW_INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DW_TF_DIR="${DW_INFRA_DIR}/terraform"
DW_ROOT="$(cd "${DW_INFRA_DIR}/.." && pwd)"
DATALAKE_ROOT="${DATALAKE_ROOT:-$(cd "${DW_ROOT}/../datalake" 2>/dev/null && pwd)}"
if [ -z "$DATALAKE_ROOT" ] || [ ! -f "$DATALAKE_ROOT/infra/common.sh" ]; then
  echo "No encuentro la carpeta datalake junto a dw (exporta DATALAKE_ROOT=/ruta/al/datalake)." >&2
  exit 1
fi
# Trae SSH_KEY, la región y live_dns() / instance_state() de la EC2.
source "$DATALAKE_ROOT/infra/common.sh"

dw_out() {
  terraform -chdir="$DW_TF_DIR" output -raw "$1" 2>/dev/null || {
    echo "No se pudo leer '$1'. ¿Ya ejecutaste terraform apply en dw/infra/terraform?" >&2
    exit 1
  }
}
