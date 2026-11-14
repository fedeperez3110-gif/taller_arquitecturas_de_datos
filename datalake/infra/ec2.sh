#!/usr/bin/env bash
# Enciende, apaga o consulta la EC2 de Airflow (para no pagar horas sin usarla).
# Uso: ./infra/ec2.sh status | start | stop | ssh
set -euo pipefail
source "$(dirname "$0")/common.sh"

ID="$(instance_id)"
case "${1:-status}" in
  stop)
    aws ec2 stop-instances --instance-ids "$ID" >/dev/null
    echo "Apagando ${ID}. Airflow volverá a arrancar solo al encenderla." ;;
  start)
    aws ec2 start-instances --instance-ids "$ID" >/dev/null
    echo "Encendiendo ${ID}..."
    aws ec2 wait instance-running --instance-ids "$ID"
    DNS="$(live_dns)"
    echo "SSH:     ssh -i ${SSH_KEY} ec2-user@${DNS}"
    echo "Airflow: http://${DNS}:8080   (espera ~1 min a que arranque)" ;;
  ssh)
    exec ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "ec2-user@$(live_dns)" ;;
  status)
    echo "Estado: $(instance_state)"
    DNS="$(live_dns)"
    [ "$DNS" != "None" ] && [ -n "$DNS" ] && echo "Airflow: http://${DNS}:8080" || true ;;
  *)
    echo "Uso: $0 status | start | stop | ssh" >&2; exit 1 ;;
esac
