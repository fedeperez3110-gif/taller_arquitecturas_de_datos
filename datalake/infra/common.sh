#!/usr/bin/env bash
# Funciones compartidas: leen los datos de la infraestructura desde Terraform.
INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${INFRA_DIR}/terraform"
ROOT_DIR="$(cd "${INFRA_DIR}/.." && pwd)"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/rss-datalake}"

tf_out() {
  terraform -chdir="$TF_DIR" output -raw "$1" 2>/dev/null || {
    echo "No se pudo leer '$1' de Terraform. ¿Ya ejecutaste 'terraform apply' en infra/terraform?" >&2
    exit 1
  }
}

instance_id() { tf_out instance_id; }

# La AWS CLI debe usar la MISMA región en la que Terraform creó los recursos,
# sin importar la región configurada con `aws configure`.
AWS_DEFAULT_REGION="$(tf_out aws_region)"
export AWS_DEFAULT_REGION AWS_REGION="$AWS_DEFAULT_REGION"

# El DNS público cambia al apagar/encender: se consulta en vivo a AWS.
live_dns() {
  aws ec2 describe-instances --instance-ids "$(instance_id)" \
    --query 'Reservations[0].Instances[0].PublicDnsName' --output text
}

instance_state() {
  aws ec2 describe-instances --instance-ids "$(instance_id)" \
    --query 'Reservations[0].Instances[0].State.Name' --output text
}
