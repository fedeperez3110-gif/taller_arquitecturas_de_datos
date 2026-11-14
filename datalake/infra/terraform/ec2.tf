# EC2 con Docker para Airflow. SSH y Airflow abiertos SOLO a las IPs permitidas.

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  allowed_cidrs = coalesce(var.allowed_cidrs, ["${chomp(data.http.my_ip.response_body)}/32"])
}

data "aws_vpc" "default" {
  default = true
}

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_security_group" "airflow" {
  name        = "${var.project}-sg"
  description = "Airflow del data lake RSS"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = local.allowed_cidrs
  }

  ingress {
    description = "Airflow UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = local.allowed_cidrs
  }

  egress {
    description = "Salida a Internet (RSS, servicios AWS, imagenes Docker)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "airflow" {
  key_name   = "${var.project}-key"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ssm_parameter.al2023_ami.value
  instance_type          = var.instance_type
  key_name               = aws_key_pair.airflow.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2.name
  vpc_security_group_ids = [aws_security_group.airflow.id]
  user_data              = file("${path.module}/user_data.sh")

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2 obligatorio
    http_put_response_hop_limit = 2          # necesario para que los contenedores lean el rol IAM
  }

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  tags = {
    Name = "${var.project}-airflow"
  }

  lifecycle {
    # Una AMI nueva publicada por AWS no debe recrear la instancia (perderías Airflow).
    ignore_changes = [ami, user_data]
  }
}
