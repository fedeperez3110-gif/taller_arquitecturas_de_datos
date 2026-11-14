# VPC por defecto (la misma de la EC2 de Airflow). Las bases quedan PRIVADAS: solo la EC2
# (por security group) puede conectarse a MySQL, DocumentDB y Redshift.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_security_group" "airflow" {
  name   = var.airflow_ec2_security_group_name
  vpc_id = data.aws_vpc.default.id
}

resource "aws_security_group" "mysql" {
  name        = "${var.project}-mysql-sg"
  description = "RDS MySQL: solo desde la EC2 de Airflow"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "MySQL desde Airflow"
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [data.aws_security_group.airflow.id]
  }
}

resource "aws_security_group" "docdb" {
  name        = "${var.project}-docdb-sg"
  description = "DocumentDB: solo desde la EC2 de Airflow"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "DocumentDB desde Airflow"
    from_port       = 27017
    to_port         = 27017
    protocol        = "tcp"
    security_groups = [data.aws_security_group.airflow.id]
  }
}

resource "aws_security_group" "redshift" {
  name        = "${var.project}-redshift-sg"
  description = "Redshift Serverless: Airflow y, opcionalmente, QuickSight"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "Redshift desde Airflow"
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [data.aws_security_group.airflow.id]
  }

  dynamic "ingress" {
    for_each = var.redshift_publicly_accessible ? [1] : []
    content {
      description = "Redshift desde QuickSight (us-east-2)"
      from_port   = 5439
      to_port     = 5439
      protocol    = "tcp"
      cidr_blocks = [var.quicksight_cidr]
    }
  }

  egress {
    description = "Salida (COPY desde S3)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
