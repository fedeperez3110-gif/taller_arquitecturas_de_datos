# Bucket privado del DW (zona raw por fuente/entidad/lote + control de lotes).

locals {
  bucket_name = "${var.project}-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.region}"
}

resource "aws_s3_bucket" "dw" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "dw" {
  bucket                  = aws_s3_bucket.dw.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "dw" {
  bucket = aws_s3_bucket.dw.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dw" {
  bucket = aws_s3_bucket.dw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

data "aws_iam_policy_document" "dw_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [aws_s3_bucket.dw.arn, "${aws_s3_bucket.dw.arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "dw" {
  bucket     = aws_s3_bucket.dw.id
  policy     = data.aws_iam_policy_document.dw_bucket.json
  depends_on = [aws_s3_bucket_public_access_block.dw]
}

# ------------------------------------------------------------------ Secretos
# Una entrada por servicio con host, puerto, usuario y contraseña. El código los lee en tiempo de
# ejecución con el rol de la EC2: no hay contraseñas en el repositorio ni en el .env.
resource "aws_secretsmanager_secret" "mysql" {
  name                    = "${var.project}/mysql"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "mysql" {
  secret_id = aws_secretsmanager_secret.mysql.id
  secret_string = jsonencode({
    host     = aws_db_instance.mysql.address
    port     = aws_db_instance.mysql.port
    username = aws_db_instance.mysql.username
    password = random_password.mysql.result
    database = aws_db_instance.mysql.db_name
  })
}

resource "aws_secretsmanager_secret" "docdb" {
  name                    = "${var.project}/docdb"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "docdb" {
  secret_id = aws_secretsmanager_secret.docdb.id
  secret_string = jsonencode({
    host     = aws_docdb_cluster.docdb.endpoint
    port     = aws_docdb_cluster.docdb.port
    username = aws_docdb_cluster.docdb.master_username
    password = random_password.docdb.result
    database = "ecommerce"
  })
}

resource "aws_secretsmanager_secret" "redshift" {
  name                    = "${var.project}/redshift"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  secret_string = jsonencode({
    host     = aws_redshiftserverless_workgroup.dw.endpoint[0].address
    port     = aws_redshiftserverless_workgroup.dw.endpoint[0].port
    username = aws_redshiftserverless_namespace.dw.admin_username
    password = random_password.redshift.result
    database = var.redshift_database
  })
}

# ------------------------------------------------------------------ Permisos de la EC2 de Airflow
data "aws_iam_role" "airflow" {
  name = var.airflow_ec2_role_name
}

data "aws_iam_policy_document" "airflow_dw" {
  statement {
    sid       = "DwBucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.dw.arn]
  }
  statement {
    sid       = "DwBucketObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.dw.arn}/*"]
  }
  statement {
    sid     = "ReadDwSecrets"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_secretsmanager_secret.mysql.arn,
      aws_secretsmanager_secret.docdb.arn,
      aws_secretsmanager_secret.redshift.arn,
    ]
  }
}

resource "aws_iam_role_policy" "airflow_dw" {
  name   = "${var.project}-airflow-access"
  role   = data.aws_iam_role.airflow.name
  policy = data.aws_iam_policy_document.airflow_dw.json
}
