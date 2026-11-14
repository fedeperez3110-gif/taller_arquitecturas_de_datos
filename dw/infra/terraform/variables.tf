variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "project" {
  description = "Prefijo de los recursos del data warehouse."
  type        = string
  default     = "ecommerce-dw"
}

# --- Plataforma compartida con el Data Lake (Airflow + dbt en la misma EC2) ---
variable "airflow_ec2_role_name" {
  description = "Rol IAM de la EC2 de Airflow creada por el proyecto datalake."
  type        = string
  default     = "rss-datalake-ec2-role"
}

variable "airflow_ec2_security_group_name" {
  description = "Security group de la EC2 de Airflow creada por el proyecto datalake."
  type        = string
  default     = "rss-datalake-sg"
}

# --- Fuentes ---
variable "mysql_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "mysql_engine_version" {
  type    = string
  default = "8.4"
}

variable "docdb_instance_class" {
  type    = string
  default = "db.t3.medium"
}

variable "docdb_engine_version" {
  type    = string
  default = "5.0.0"
}

# --- Redshift Serverless ---
variable "redshift_base_capacity" {
  description = "RPU base. 4 es el mínimo disponible en us-east-2."
  type        = number
  default     = 4
}

variable "redshift_database" {
  type    = string
  default = "ecommerce"
}

variable "redshift_publicly_accessible" {
  description = "true solo para conectar QuickSight sin VPC connection (acceso limitado a quicksight_cidr)."
  type        = bool
  default     = false
}

variable "quicksight_cidr" {
  description = "Rango de IP de QuickSight en us-east-2 (documentación de AWS)."
  type        = string
  default     = "52.15.247.160/27"
}
