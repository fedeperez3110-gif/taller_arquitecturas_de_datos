variable "aws_region" {
  description = "Región AWS (debe soportar Comprehend, Glue y Athena)."
  type        = string
  default     = "us-east-2"
}

variable "project" {
  description = "Prefijo para nombrar recursos."
  type        = string
  default     = "rss-datalake"
}

variable "glue_database" {
  description = "Base de datos del Glue Data Catalog."
  type        = string
  default     = "rss_datalake"
}

variable "instance_type" {
  description = "Tipo de EC2 para Airflow. t3.large (8 GiB) es holgado; t3.medium (4 GiB) funciona justo."
  type        = string
  default     = "t3.large"
}

variable "ssh_public_key_path" {
  description = "Llave pública SSH (la privada nunca sale de tu computador)."
  type        = string
  default     = "~/.ssh/rss-datalake.pub"
}

variable "allowed_cidrs" {
  description = "IPs que pueden entrar por SSH (22) y a Airflow (8080). null = detectar tu IP pública actual."
  type        = list(string)
  default     = null
}

variable "force_destroy_bucket" {
  description = "Permite que 'terraform destroy' borre el bucket aunque tenga datos."
  type        = bool
  default     = true
}

variable "athena_bytes_scanned_cutoff" {
  description = "Límite de bytes escaneados por consulta en Athena (control de costos)."
  type        = number
  default     = 1073741824 # 1 GiB
}

variable "alert_email" {
  description = "Correo para la alerta de presupuesto. null = no crear presupuesto."
  type        = string
  default     = null
}

variable "monthly_budget_usd" {
  description = "Presupuesto mensual (USD) para la alerta de costos."
  type        = number
  default     = 10
}
