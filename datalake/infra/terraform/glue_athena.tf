# Base de datos del catálogo. Las TABLAS de Silver y Gold se agregan en la Fase 6.
resource "aws_glue_catalog_database" "datalake" {
  name        = var.glue_database
  description = "Data lake de noticias RSS (Silver y Gold)"
}

# Workgroup propio: fija la ruta de resultados, cifra y limita bytes escaneados por consulta.
resource "aws_athena_workgroup" "datalake" {
  name          = var.project
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}
