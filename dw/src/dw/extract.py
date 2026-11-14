"""Extracción inicial (full) e incremental por lote desde RDS MySQL y DocumentDB hacia S3.

Por cada entidad escribe raw/<fuente>/<entidad>/batch_id=<id>/part-00000.parquet y, al final, el
manifiesto control/batches/batch_id=<id>/extract_<fuente>.json con fecha de extracción, lote,
cantidad de registros, modo y marcas de avance.

Idempotencia: si el manifiesto ya existe, la fuente NO se vuelve a consultar (reintentos y
reprocesos usan los archivos guardados). Un lote nunca escribe en la carpeta de otro lote.

Marcas de avance:
  * customers / products: updated_at >= marca (inclusive: no se pierden cambios del mismo segundo;
    lo repetido se descarta en staging).
  * orders / order_items / reviews / navigation_events: id > marca (registros solo se insertan).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
from bson import json_util
from pymysql.cursors import SSDictCursor

from dw import batches, s3io
from dw.connections import docdb_client, docdb_database, mysql_connect
from dw.entities import BY_SOURCE, Entity
from dw.settings import Settings

log = logging.getLogger(__name__)
FULL, INCREMENTAL = "full", "incremental"


def _ctx(batch_id: str, source: str, entity: str = "-") -> str:
    return f"[batch_id={batch_id}][source={source}][entity={entity}]"


def _wm_value(entity: Entity, raw):
    if raw is None:
        return None
    if entity.watermark_kind == "timestamp":
        return datetime.fromisoformat(raw)
    return int(raw)


def _wm_out(value):
    if value is None:
        return None
    return value.isoformat(sep=" ") if isinstance(value, datetime) else int(value)


def _to_table(entity: Entity, rows: list[dict], batch_id: str, extracted_at: datetime) -> pa.Table:
    meta = {"_batch_id": batch_id, "_extracted_at": extracted_at.replace(tzinfo=None),
            "_source": f"{entity.source}.{entity.name}"}
    data = [{**{c: r.get(c) for c, _ in entity.columns}, **meta} for r in rows]
    return pa.Table.from_pylist(data, schema=entity.arrow_schema())


def _normalize_mysql(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if isinstance(v, int) and not isinstance(v, bool) and k == "is_active":
            v = bool(v)
        out[k] = v
    return out


def _fetch_mysql(conn, entity: Entity, wm) -> list[dict]:
    cols = ", ".join(c for c, _ in entity.columns)
    table = entity.name
    if wm is None:
        sql, params = f"SELECT {cols} FROM {table} ORDER BY {entity.key}", None
    elif entity.watermark_kind == "timestamp":
        sql, params = f"SELECT {cols} FROM {table} WHERE {entity.watermark} >= %s ORDER BY {entity.key}", (wm,)
    else:
        sql, params = f"SELECT {cols} FROM {table} WHERE {entity.watermark} > %s ORDER BY {entity.key}", (wm,)
    with conn.cursor(SSDictCursor) as cur:
        cur.execute(sql, params)
        return [_normalize_mysql(r) for r in cur.fetchall()]


def _naive_utc(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def flatten_document(entity_name: str, doc: dict) -> dict:
    """Igual que DE-DW: campos seleccionados + documento completo en document_json."""
    customer = doc.get("customer") or {}
    product = doc.get("product") or {}
    if entity_name == "reviews":
        row = {"review_id": doc.get("review_id"), "customer_id": customer.get("customer_id"),
               "product_id": product.get("product_id"), "rating": doc.get("rating"),
               "reviewed_at": _naive_utc(doc.get("reviewed_at"))}
    else:
        device = doc.get("device") or {}
        row = {"event_id": doc.get("event_id"), "event_type": doc.get("event_type"),
               "occurred_at": _naive_utc(doc.get("occurred_at")),
               "customer_id": customer.get("customer_id"), "product_id": product.get("product_id"),
               "sales_channel": device.get("sales_channel")}
    row["document_json"] = json_util.dumps(doc)
    return row


def _fetch_mongo(db, entity: Entity, wm) -> list[dict]:
    query = {} if wm is None else {entity.watermark: {"$gt": wm}}
    cursor = db[entity.name].find(query).sort(entity.key, 1).batch_size(1000)
    return [flatten_document(entity.name, d) for d in cursor]


def _max_watermark(entity: Entity, rows: list[dict], previous):
    values = [r[entity.watermark] for r in rows if r.get(entity.watermark) is not None]
    candidates = values + ([previous] if previous is not None else [])
    return max(candidates) if candidates else None


def extract_source(settings: Settings, source: str, batch_id: str, mode: str = "auto",
                   s3=None, connection_factory=None, now: datetime | None = None) -> dict:
    """Extrae todas las entidades de una fuente. Devuelve el manifiesto."""
    batches.validate_batch_id(batch_id)
    s3 = s3 or s3io.client(settings.region)
    manifest_key = batches.control_key(batch_id, f"extract_{source}")
    if s3io.exists(s3, settings.bucket, manifest_key):
        log.info("%s ya extraído; se reutiliza S3 sin consultar la fuente", _ctx(batch_id, source))
        return {**s3io.read_json(s3, settings.bucket, manifest_key), "action": "REUSED"}

    ref_batch, watermarks = (None, {}) if mode == FULL else batches.previous_watermarks(
        s3, settings.bucket, source, batch_id)
    effective_mode = INCREMENTAL if ref_batch else FULL
    extracted_at = now or datetime.now(timezone.utc)

    entities = BY_SOURCE[source]
    results = []
    if source == "mysql":
        conn = (connection_factory or mysql_connect)(settings)
        try:
            fetched = {e.name: _fetch_mysql(conn, e, _wm_value(e, watermarks.get(e.name))) for e in entities}
        finally:
            conn.close()
    else:
        client = (connection_factory or docdb_client)(settings)
        try:
            db = docdb_database(client, settings) if connection_factory is None else client["ecommerce"]
            fetched = {e.name: _fetch_mongo(db, e, _wm_value(e, watermarks.get(e.name))) for e in entities}
        finally:
            client.close()

    for e in entities:
        rows = fetched[e.name]
        wm_from = _wm_value(e, watermarks.get(e.name))
        key = None
        if rows:
            key = e.s3_prefix(batch_id) + "part-00000.parquet"
            s3io.put_parquet(s3, settings.bucket, key, _to_table(e, rows, batch_id, extracted_at))
        results.append({
            "entity": e.name, "raw_table": e.raw_table, "rows": len(rows), "s3_key": key,
            "mode": effective_mode, "watermark_column": e.watermark,
            "watermark_from": _wm_out(wm_from), "watermark_to": _wm_out(_max_watermark(e, rows, wm_from)),
        })
        log.info("%s modo=%s registros=%s marca=%s -> %s", _ctx(batch_id, source, e.name),
                 effective_mode, len(rows), _wm_out(wm_from), results[-1]["watermark_to"])

    manifest = {"batch_id": batch_id, "source": source, "mode": effective_mode,
                "watermarks_from_batch": ref_batch, "extracted_at_utc": extracted_at.isoformat(),
                "total_rows": sum(r["rows"] for r in results), "entities": results}
    s3io.put_json(s3, settings.bucket, manifest_key, manifest)   # al final: marca de commit
    return {**manifest, "action": "EXTRACTED"}


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)
