# Amazon Redshift Serverless: cobra por RPU-hora solo mientras procesa consultas.

resource "random_password" "redshift" {
  length           = 24
  special          = true
  override_special = "!#%^*-_=+" # Redshift no admite ' " / \ @ ni espacios
  min_upper        = 1
  min_lower        = 1
  min_numeric      = 1
}

# Rol que usa Redshift para leer S3 en el COPY (requisito 3.3.2).
data "aws_iam_policy_document" "redshift_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_copy" {
  name               = "${var.project}-redshift-copy-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_trust.json
}

data "aws_iam_policy_document" "redshift_s3_read" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.dw.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.dw.arn}/raw/*"] # solo la zona raw
  }
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  name   = "${var.project}-redshift-s3-read"
  role   = aws_iam_role.redshift_copy.id
  policy = data.aws_iam_policy_document.redshift_s3_read.json
}

resource "aws_redshiftserverless_namespace" "dw" {
  namespace_name       = var.project
  db_name              = var.redshift_database
  admin_username       = "dw_admin"
  admin_user_password  = random_password.redshift.result
  iam_roles            = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "dw" {
  workgroup_name       = var.project
  namespace_name       = aws_redshiftserverless_namespace.dw.namespace_name
  base_capacity        = var.redshift_base_capacity
  subnet_ids           = data.aws_subnets.default.ids
  security_group_ids   = [aws_security_group.redshift.id]
  publicly_accessible  = var.redshift_publicly_accessible
  enhanced_vpc_routing = false

  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }
}

# Límite de gasto: corta el uso si se superan estas RPU-hora en el día.
resource "aws_redshiftserverless_usage_limit" "daily" {
  resource_arn  = aws_redshiftserverless_workgroup.dw.arn
  usage_type    = "serverless-compute"
  amount        = 16 # RPU-hora por día (≈ 6 USD); si se supera, Redshift se pausa hasta el día siguiente
  period        = "daily"
  breach_action = "deactivate"
}
