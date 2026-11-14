#!/bin/bash
# Se ejecuta UNA vez cuando arranca la EC2 (Amazon Linux 2023).
# Instala Docker, el plugin Docker Compose y git.
set -euxo pipefail

dnf update -y
dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user

mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo "user_data terminado" > /home/ec2-user/BOOTSTRAP_OK
