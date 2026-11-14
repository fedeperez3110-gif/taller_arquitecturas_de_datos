output "aws_region" {
  value = data.aws_region.current.region
}

output "bucket_name" {
  value = aws_s3_bucket.dw.bucket
}

output "mysql_identifier" {
  value = aws_db_instance.mysql.identifier
}

output "docdb_cluster_identifier" {
  value = aws_docdb_cluster.docdb.cluster_identifier
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.dw.workgroup_name
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.dw.endpoint[0].address
}

output "redshift_copy_role_arn" {
  value = aws_iam_role.redshift_copy.arn
}

output "env_values" {
  description = "Contenido de dw/.env en la EC2 (sin contraseñas: se leen de Secrets Manager)"
  value       = <<-EOT
    DW_BUCKET=${aws_s3_bucket.dw.bucket}
    DW_REGION=${data.aws_region.current.region}
    DW_MYSQL_SECRET=${aws_secretsmanager_secret.mysql.name}
    DW_DOCDB_SECRET=${aws_secretsmanager_secret.docdb.name}
    DW_REDSHIFT_SECRET=${aws_secretsmanager_secret.redshift.name}
    DW_REDSHIFT_IAM_ROLE=${aws_iam_role.redshift_copy.arn}
  EOT
}
