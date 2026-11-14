# Definiciones EXPLÍCITAS de las tablas de Silver y Gold en el Glue Data Catalog.
# (Se eligió esto en lugar de crawlers: el esquema queda versionado y no lo infiere un proceso
#  externo.) Las PARTICIONES no se declaran aquí: las registra el DAG en cada lote
#  (tarea register_partitions -> src/datalake/catalog.py) después de escribir los archivos.
#
# Todas las tablas: Parquet (snappy), un archivo por partición, partición string.

locals {
  ts = "timestamp"

  glue_tables = {
    # ------------------------------------------------------------ Silver
    silver_news = {
      location    = "silver/news/"
      description = "Noticias vigentes: una fila por news_id (versión vigente)"
      partition   = "published_date"
      columns = [
        ["news_id", "string"], ["id_strategy", "string"], ["id_source_value", "string"],
        ["guid", "string"], ["link", "string"], ["title", "string"], ["description", "string"],
        ["title_raw", "string"], ["description_raw", "string"], ["published_at_raw", "string"],
        ["published_at_utc", local.ts], ["pub_date_status", "string"], ["media", "string"],
        ["sections", "array<string>"], ["source_ids", "array<string>"], ["content_hash", "string"],
        ["version", "int"], ["first_seen_run_id", "string"], ["first_seen_at_utc", local.ts],
        ["last_seen_run_id", "string"], ["last_seen_at_utc", local.ts], ["content_run_id", "string"],
        ["content_ingested_at_utc", local.ts], ["bronze_key", "string"],
      ]
    }
    silver_news_entities = {
      location    = "silver/news_entities/"
      description = "Entidades de Comprehend: una fila por (noticia, tipo, entidad normalizada)"
      partition   = "published_date"
      columns = [
        ["news_id", "string"], ["content_hash", "string"], ["entity_type", "string"],
        ["entity_text", "string"], ["entity_normalized", "string"], ["mention_count", "int"],
        ["max_score", "double"], ["media", "string"], ["sections", "array<string>"],
        ["published_at_utc", local.ts],
      ]
    }
    silver_news_key_phrases = {
      location    = "silver/news_key_phrases/"
      description = "Frases clave de Comprehend: una fila por (noticia, frase normalizada)"
      partition   = "published_date"
      columns = [
        ["news_id", "string"], ["content_hash", "string"], ["phrase_text", "string"],
        ["phrase_normalized", "string"], ["mention_count", "int"], ["max_score", "double"],
        ["media", "string"], ["sections", "array<string>"], ["published_at_utc", local.ts],
      ]
    }
    silver_quarantine = {
      location    = "quarantine/silver_rejected/"
      description = "Registros rechazados por lote, con motivo y referencia a Bronze"
      partition   = "run_id"
      # run_id NO se repite como columna: viene de la partición (Athena no admite nombres duplicados)
      columns = [
        ["source_id", "string"], ["section", "string"], ["bronze_key", "string"],
        ["item_position", "int"], ["reasons", "array<string>"], ["guid", "string"], ["link", "string"],
        ["title_raw", "string"], ["description_raw", "string"], ["published_at_raw", "string"],
        ["rejected_at_utc", local.ts],
      ]
    }
    batch_log = {
      location    = "control/batch_log/"
      description = "Conteos de entrada y resultado por lote y fuente"
      partition   = "run_id"
      columns = [
        ["source_id", "string"], ["bronze_status", "string"],
        ["items_received", "int"], ["rejected", "int"], ["duplicates_in_batch", "int"], ["new", "int"],
        ["updated", "int"], ["unchanged", "int"], ["stale", "int"], ["processed_at_utc", local.ts],
      ]
    }
    # ------------------------------------------------------------ Gold
    gold_news_by_section = {
      location    = "gold/news_by_section/"
      description = "P1: noticias distintas por día, medio y sección"
      partition   = "published_date"
      columns     = [["media", "string"], ["section", "string"], ["news_count", "int"]]
    }
    gold_news_by_day = {
      location    = "gold/news_by_day/"
      description = "P1: noticias distintas por día y medio (sin repetir por sección)"
      partition   = "published_date"
      columns     = [["media", "string"], ["news_count", "int"], ["sections_count", "int"]]
    }
    gold_entity_daily = {
      location    = "gold/entity_daily/"
      description = "P2/P4: noticias distintas que mencionan cada entidad, por día"
      partition   = "published_date"
      columns = [
        ["entity_type", "string"], ["entity_normalized", "string"], ["entity_text", "string"],
        ["news_count", "int"], ["mention_count", "int"],
      ]
    }
    gold_phrase_by_section = {
      location    = "gold/phrase_by_section/"
      description = "P3: noticias distintas con cada frase clave, por sección y día"
      partition   = "published_date"
      columns = [
        ["section", "string"], ["phrase_normalized", "string"], ["phrase_text", "string"],
        ["news_count", "int"],
      ]
    }
  }
}

resource "aws_glue_catalog_table" "tables" {
  for_each = local.glue_tables

  name          = each.key
  database_name = aws_glue_catalog_database.datalake.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys {
    name = each.value.partition
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.bucket}/${each.value.location}"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

output "glue_tables" {
  value = sort(keys(local.glue_tables))
}
