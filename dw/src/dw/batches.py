"""Identificación de lotes, marcas de avance y estado.

Estructura en S3:
  raw/<fuente>/<entidad>/batch_id=<id>/part-00000.parquet   datos extraídos (inmutables)
  control/batches/batch_id=<id>/extract_<fuente>.json         manifiesto de extracción (marca de commit)
  control/batches/batch_id=<id>/load.json                     resultado del COPY a Redshift
  control/batches/batch_id=<id>/status.json                   estado final del lote (COMPLETE / FAILED)
  control/airflow_runs/<run de Airflow>.json                  batch_id asignado a cada ejecución

Cuándo avanza la marca: una extracción incremental parte de las marcas (watermark_to) del ÚLTIMO
lote con status COMPLETE. Si un lote falla, la marca no avanza y el siguiente vuelve a extraer
desde el último lote completo (los duplicados se descartan en staging).
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from dw import s3io

BATCH_RE = re.compile(r"^\d{8}T\d{6}Z$")
COMPLETE, FAILED = "COMPLETE", "FAILED"


def new_batch_id(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def validate_batch_id(batch_id: str) -> str:
    if not BATCH_RE.match(batch_id or ""):
        raise ValueError(f"batch_id inválido '{batch_id}' (formato AAAAMMDDTHHMMSSZ)")
    return batch_id


def control_key(batch_id: str, name: str) -> str:
    return f"control/batches/batch_id={batch_id}/{name}.json"


def resolve_batch_id(s3, bucket: str, airflow_run_id: str, now: datetime | None = None) -> str:
    """Hora real de inicio, fijada la primera vez para que los reintentos usen el mismo lote."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", airflow_run_id)
    key = f"control/airflow_runs/{safe}.json"
    if s3io.exists(s3, bucket, key):
        return validate_batch_id(s3io.read_json(s3, bucket, key)["batch_id"])
    batch_id = new_batch_id(now)
    s3io.put_json(s3, bucket, key, {"airflow_run_id": airflow_run_id, "batch_id": batch_id})
    return batch_id


def list_batches(s3, bucket: str) -> list[str]:
    ids = []
    for p in s3io.list_prefixes(s3, bucket, "control/batches/"):
        m = re.search(r"batch_id=(\d{8}T\d{6}Z)/$", p)
        if m:
            ids.append(m.group(1))
    return sorted(ids)


def batch_status(s3, bucket: str, batch_id: str) -> str | None:
    key = control_key(batch_id, "status")
    return s3io.read_json(s3, bucket, key).get("status") if s3io.exists(s3, bucket, key) else None


def last_complete_batch(s3, bucket: str, before: str | None = None) -> str | None:
    for bid in reversed(list_batches(s3, bucket)):
        if before and bid >= before:
            continue
        if batch_status(s3, bucket, bid) == COMPLETE:
            return bid
    return None


def previous_watermarks(s3, bucket: str, source: str, batch_id: str) -> tuple[str | None, dict]:
    """(lote de referencia, {entidad: watermark_to}) del último lote completo anterior a batch_id."""
    ref = last_complete_batch(s3, bucket, before=batch_id)
    if ref is None:
        return None, {}
    key = control_key(ref, f"extract_{source}")
    if not s3io.exists(s3, bucket, key):
        return None, {}
    manifest = s3io.read_json(s3, bucket, key)
    return ref, {e["entity"]: e["watermark_to"] for e in manifest["entities"]}


def record_status(s3, bucket: str, batch_id: str, status: str, detail: dict) -> None:
    s3io.put_json(s3, bucket, control_key(batch_id, "status"), {
        "batch_id": batch_id, "status": status,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(), **detail,
    })
