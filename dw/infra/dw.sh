#!/usr/bin/env bash
# Enciende, apaga o consulta las fuentes del DW para no pagar horas sin usarlas.
#   RDS MySQL y DocumentDB: se pueden detener (AWS los vuelve a encender solos a los 7 días).
#   Redshift Serverless: no se apaga; solo cobra RPU-hora mientras ejecuta consultas
#   (y el almacenamiento). El límite diario de Terraform lo desactiva si se excede.
# Uso: ./infra/dw.sh status | start | stop | env
set -euo pipefail
source "$(dirname "$0")/common.sh"

DB="$(dw_out mysql_identifier)"
DOC="$(dw_out docdb_cluster_identifier)"
mysql_state() { aws rds describe-db-instances --db-instance-identifier "$DB" --query 'DBInstances[0].DBInstanceStatus' --output text; }
docdb_state() { aws docdb describe-db-clusters --db-cluster-identifier "$DOC" --query 'DBClusters[0].Status' --output text; }

case "${1:-status}" in
  status)
    echo "MySQL      ($DB): $(mysql_state)"
    echo "DocumentDB ($DOC): $(docdb_state)"
    echo "Redshift   ($(dw_out redshift_workgroup)): $(aws redshift-serverless get-workgroup \
      --workgroup-name "$(dw_out redshift_workgroup)" --query 'workgroup.status' --output text)" ;;
  stop)
    [ "$(mysql_state)" = "available" ] && aws rds stop-db-instance --db-instance-identifier "$DB" >/dev/null && echo "Deteniendo MySQL"
    [ "$(docdb_state)" = "available" ] && aws docdb stop-db-cluster --db-cluster-identifier "$DOC" >/dev/null && echo "Deteniendo DocumentDB"
    echo "Tarda unos minutos. Consulta con: $0 status" ;;
  start)
    [ "$(mysql_state)" = "stopped" ] && aws rds start-db-instance --db-instance-identifier "$DB" >/dev/null && echo "Encendiendo MySQL"
    [ "$(docdb_state)" = "stopped" ] && aws docdb start-db-cluster --db-cluster-identifier "$DOC" >/dev/null && echo "Encendiendo DocumentDB"
    echo "Esperando a que queden disponibles (5-10 min)..."
    aws rds wait db-instance-available --db-instance-identifier "$DB"
    until [ "$(docdb_state)" = "available" ]; do sleep 20; done
    echo "Fuentes disponibles." ;;
  env)
    terraform -chdir="$DW_TF_DIR" output -raw env_values ;;
  *)
    echo "Uso: $0 status | start | stop | env" >&2; exit 1 ;;
esac
