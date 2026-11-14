# Rol de la EC2: Airflow obtiene credenciales temporales de aquí.
# No hay llaves de AWS en el código, en .env ni en la instancia.

data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  name               = "${var.project}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

locals {
  account   = data.aws_caller_identity.current.account_id
  region    = data.aws_region.current.region
  glue_base = "arn:aws:glue:${local.region}:${local.account}"
}

# Mínimo privilegio: solo el bucket, la base de Glue y el workgroup de este proyecto.
data "aws_iam_policy_document" "ec2_access" {
  statement {
    sid       = "DataLakeBucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.datalake.arn]
  }

  statement {
    sid       = "DataLakeBucketObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.datalake.arn}/*"]
  }

  statement {
    sid = "ComprehendDetect"
    actions = [
      "comprehend:DetectEntities",
      "comprehend:DetectKeyPhrases",
      "comprehend:DetectDominantLanguage",
      "comprehend:BatchDetectEntities",
      "comprehend:BatchDetectKeyPhrases",
      "comprehend:BatchDetectDominantLanguage",
    ]
    resources = ["*"] # Comprehend no admite restringir estas acciones por recurso
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition",
      "glue:BatchCreatePartition", "glue:UpdatePartition", "glue:BatchUpdatePartition",
      "glue:DeletePartition", "glue:BatchDeletePartition",
    ]
    resources = [
      "${local.glue_base}:catalog",
      "${local.glue_base}:database/${var.glue_database}",
      "${local.glue_base}:table/${var.glue_database}/*",
    ]
  }

  statement {
    sid = "AthenaQueries"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution",
      "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.datalake.arn]
  }
}

resource "aws_iam_role_policy" "ec2_access" {
  name   = "${var.project}-access"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.ec2_access.json
}

# Permite entrar a la EC2 con Session Manager desde la consola, como alternativa a SSH.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project}-ec2-profile"
  role = aws_iam_role.ec2.name
}
