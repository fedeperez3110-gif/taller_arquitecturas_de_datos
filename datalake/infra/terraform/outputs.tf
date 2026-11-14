output "aws_region" {
  value = data.aws_region.current.region
}

output "bucket_name" {
  value = aws_s3_bucket.datalake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.datalake.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.datalake.name
}

output "instance_id" {
  value = aws_instance.airflow.id
}

output "public_dns" {
  description = "Cambia cada vez que la EC2 se apaga y enciende; usa ./infra/ec2.sh status"
  value       = aws_instance.airflow.public_dns
}

output "ssh_command" {
  value = "ssh -i ${trimsuffix(pathexpand(var.ssh_public_key_path), ".pub")} ec2-user@${aws_instance.airflow.public_dns}"
}

output "airflow_url" {
  value = "http://${aws_instance.airflow.public_dns}:8080"
}

output "allowed_cidrs" {
  value = local.allowed_cidrs
}

output "env_values" {
  description = "Valores para el .env de la EC2"
  value       = <<-EOT
    DATALAKE_BUCKET=${aws_s3_bucket.datalake.bucket}
    AWS_DEFAULT_REGION=${data.aws_region.current.region}
    GLUE_DATABASE=${aws_glue_catalog_database.datalake.name}
    ATHENA_WORKGROUP=${aws_athena_workgroup.datalake.name}
  EOT
}
