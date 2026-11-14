"""Lectura/escritura de Parquet en S3 con esquemas explícitos.

Publicación segura: cada partición se guarda como UN solo archivo con nombre fijo
(`part-00000.parquet`). Un PUT en S3 reemplaza el objeto de forma atómica, así que
Athena ve la versión anterior completa o la nueva completa, nunca un archivo a medias
ni dos copias de la misma partición.
"""
from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

from datalake import s3_utils

PART_FILE = "part-00000.parquet"
UNKNOWN_DATE = "unknown"   # valor de partición para noticias sin fecha de publicación válida

TS = pa.timestamp("ms", tz="UTC")

NEWS_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("id_strategy", pa.string()),            # GUID | LINK
    ("id_source_value", pa.string()),        # valor normalizado del que se derivó news_id
    ("guid", pa.string()),
    ("link", pa.string()),
    ("title", pa.string()),                  # limpio (sin HTML)
    ("description", pa.string()),            # limpio (sin HTML)
    ("title_raw", pa.string()),
    ("description_raw", pa.string()),
    ("published_at_raw", pa.string()),       # valor original del RSS
    ("published_at_utc", TS),
    ("pub_date_status", pa.string()),        # OK | TZ_ASSUMED | MISSING | INVALID | FUTURE
    ("media", pa.string()),
    ("sections", pa.list_(pa.string())),     # todas las secciones en que apareció
    ("source_ids", pa.list_(pa.string())),
    ("content_hash", pa.string()),           # hash de título+descripción limpios (texto analizado)
    ("version", pa.int32()),                 # sube cuando cambia content_hash
    ("first_seen_run_id", pa.string()),
    ("first_seen_at_utc", TS),
    ("last_seen_run_id", pa.string()),
    ("last_seen_at_utc", TS),
    ("content_run_id", pa.string()),         # lote del que viene el contenido vigente
    ("content_ingested_at_utc", TS),
    ("bronze_key", pa.string()),             # feed.xml de Bronze del contenido vigente
])
# La partición published_date (AAAA-MM-DD | unknown) NO va dentro del archivo:
# Athena/Glue la leen de la ruta  silver/news/published_date=<valor>/part-00000.parquet

# Una fila por (noticia, entidad normalizada, tipo): las repeticiones dentro del mismo texto
# se acumulan en mention_count y NO generan filas extra (evita inflar la presencia de una entidad).
ENTITY_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("content_hash", pa.string()),           # texto del que salió (debe coincidir con silver/news)
    ("entity_type", pa.string()),            # PERSON | ORGANIZATION | LOCATION
    ("entity_text", pa.string()),            # forma más frecuente en el texto
    ("entity_normalized", pa.string()),      # minúsculas, sin tildes ni espacios extra (clave de agrupación)
    ("mention_count", pa.int32()),           # veces que aparece en ese texto
    ("max_score", pa.float64()),             # confianza máxima de Comprehend
    ("media", pa.string()),
    ("sections", pa.list_(pa.string())),
    ("published_at_utc", TS),
])

KEY_PHRASE_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("content_hash", pa.string()),
    ("phrase_text", pa.string()),
    ("phrase_normalized", pa.string()),
    ("mention_count", pa.int32()),
    ("max_score", pa.float64()),
    ("media", pa.string()),
    ("sections", pa.list_(pa.string())),
    ("published_at_utc", TS),
])

# ---------------------------------------------------------------- Gold (todas particionadas por published_date)
# P1. Noticias por día, medio y sección. Granularidad: (día, medio, sección).
#     Una noticia presente en 2 secciones cuenta 1 vez en CADA sección.
GOLD_NEWS_BY_SECTION_SCHEMA = pa.schema([
    ("media", pa.string()),
    ("section", pa.string()),
    ("news_count", pa.int32()),              # noticias distintas
])
# P1 (total). Noticias distintas por día y medio (sin repetir por sección).
GOLD_NEWS_BY_DAY_SCHEMA = pa.schema([
    ("media", pa.string()),
    ("news_count", pa.int32()),
    ("sections_count", pa.int32()),          # secciones distintas con publicaciones ese día
])
# P2 y P4. Presencia de entidades por día. Granularidad: (día, tipo, entidad normalizada).
GOLD_ENTITY_DAILY_SCHEMA = pa.schema([
    ("entity_type", pa.string()),
    ("entity_normalized", pa.string()),
    ("entity_text", pa.string()),            # forma más frecuente ese día
    ("news_count", pa.int32()),              # noticias distintas que la mencionan (métrica de presencia)
    ("mention_count", pa.int32()),           # menciones totales (informativo; NO usar para presencia)
])
# P3. Frases clave por sección y día. Granularidad: (día, sección, frase normalizada).
GOLD_PHRASE_BY_SECTION_SCHEMA = pa.schema([
    ("section", pa.string()),
    ("phrase_normalized", pa.string()),
    ("phrase_text", pa.string()),
    ("news_count", pa.int32()),
])

QUARANTINE_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("source_id", pa.string()),
    ("section", pa.string()),
    ("bronze_key", pa.string()),
    ("item_position", pa.int32()),           # posición del <item> dentro del feed
    ("reasons", pa.list_(pa.string())),
    ("guid", pa.string()),
    ("link", pa.string()),
    ("title_raw", pa.string()),
    ("description_raw", pa.string()),
    ("published_at_raw", pa.string()),
    ("rejected_at_utc", TS),
])

BATCH_LOG_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("source_id", pa.string()),
    ("bronze_status", pa.string()),
    ("items_received", pa.int32()),
    ("rejected", pa.int32()),
    ("duplicates_in_batch", pa.int32()),     # repetidas dentro del mismo lote (p. ej. en 2 feeds)
    ("new", pa.int32()),
    ("updated", pa.int32()),
    ("unchanged", pa.int32()),               # ya existían con el mismo contenido
    ("stale", pa.int32()),                   # contenido más antiguo que el vigente (reproceso viejo)
    ("processed_at_utc", TS),
])


def table_from_rows(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


def write_table(s3, bucket: str, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    s3_utils.put_bytes(s3, bucket, key, buf.getvalue(), "application/vnd.apache.parquet")


def read_table(s3, bucket: str, key: str) -> pa.Table:
    return pq.read_table(io.BytesIO(s3_utils.read_bytes(s3, bucket, key)))


def partition_values(key: str) -> dict[str, str]:
    """'silver/news/published_date=2026-11-11/part-00000.parquet' -> {'published_date': '2026-11-11'}"""
    return dict(p.split("=", 1) for p in key.split("/") if "=" in p)


def read_dataset(s3, bucket: str, prefix: str, schema: pa.Schema) -> list[dict]:
    """Lee todos los Parquet bajo un prefijo como lista de dicts, agregando las
    columnas de partición tomadas de la ruta."""
    rows: list[dict] = []
    for key in s3_utils.list_keys(s3, bucket, prefix):
        if not key.endswith(".parquet"):
            continue
        table = read_table(s3, bucket, key)
        names = [n for n in schema.names if n in table.column_names]
        parts = partition_values(key[len(prefix):])
        for row in table.select(names).to_pylist():
            row.update(parts)
            rows.append(row)
    return rows


def publish_partitioned(s3, bucket: str, prefix: str, schema, rows_by_partition: dict[str, list[dict]],
                        sort_key) -> list[str]:
    """Escribe cada partición solo si su contenido cambió; borra las que quedaron vacías."""
    existing_parts = {
        partition_values(k[len(prefix):])["published_date"]
        for k in s3_utils.list_keys(s3, bucket, prefix) if k.endswith(".parquet")
    }
    written = []
    for part in sorted(existing_parts | set(rows_by_partition)):
        key = f"{prefix}published_date={part}/{PART_FILE}"
        rows = sorted(rows_by_partition.get(part, []), key=sort_key)
        if not rows:
            if part in existing_parts:      # la partición quedó sin filas (p. ej. la noticia cambió de fecha)
                s3_utils.delete_key(s3, bucket, key)
                written.append(part)
            continue
        if part in existing_parts and read_table(s3, bucket, key).to_pylist() == \
                table_from_rows(rows, schema).to_pylist():
            continue  # sin cambios: no se reescribe
        write_table(s3, bucket, key, table_from_rows(rows, schema))
        written.append(part)
    return written
