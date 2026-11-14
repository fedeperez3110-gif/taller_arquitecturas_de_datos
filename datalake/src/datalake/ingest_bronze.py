"""Ingesta de feeds RSS a la capa Bronze (requisitos 4.1 y 4.2).

Organización en S3 (un lote = un run_id; una carpeta por fuente):

    bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/feed.xml
    bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/manifest.json
    control/runs/run_id=<id>/bronze.json        <- resumen del lote completo
    quarantine/bronze_rejected/...               <- respuestas no válidas (trazabilidad)

Reglas de idempotencia:
  * El manifest.json se escribe AL FINAL, después de feed.xml. Funciona como
    "marca de commit": si existe con estado exitoso, la fuente ya fue ingerida.
  * Reintentar el mismo run_id NO vuelve a consultar las fuentes exitosas ni crea
    otra copia; solo reintenta las que fallaron.
  * Un run_id nuevo es una extracción nueva y nunca sobrescribe otros lotes.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from xml.etree.ElementTree import ParseError

import requests
from defusedxml import DefusedXmlException
from defusedxml import ElementTree as SafeET

from datalake import s3_utils
from datalake.config import Settings, Source, load_settings

log = logging.getLogger(__name__)

RUN_ID_RE = re.compile(r"^\d{8}T\d{6}Z$")

# Estados por fuente
OK = "OK"                        # feed válido con noticias
OK_EMPTY = "OK_EMPTY_FEED"       # feed válido pero sin <item> (NO es lo mismo que un error)
EMPTY_RESPONSE = "EMPTY_RESPONSE"
HTTP_ERROR = "HTTP_ERROR"
NETWORK_ERROR = "NETWORK_ERROR"
MALFORMED_XML = "MALFORMED_XML"
SUCCESS_STATUSES = {OK, OK_EMPTY}

# Estados del lote
RUN_COMPLETE, RUN_PARTIAL, RUN_FAILED = "COMPLETE", "PARTIAL", "FAILED"


# --------------------------------------------------------------------------- helpers
def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"run_id inválido '{run_id}'. Formato esperado: AAAAMMDDTHHMMSSZ")
    return run_id


def resolve_batch_run_id(s3, bucket: str, airflow_run_id: str, now: datetime | None = None) -> str:
    """run_id del lote para una ejecución de Airflow, basado en la HORA REAL de inicio.

    La primera vez se genera con la hora actual y se guarda en control/airflow_runs/; los
    reintentos y reejecuciones (clear) de la MISMA ejecución de Airflow leen el valor guardado,
    así que reutilizan el mismo lote (idempotencia) aunque la ejecución corra tarde.
    """
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", airflow_run_id)
    key = f"control/airflow_runs/{safe}.json"
    if s3_utils.object_exists(s3, bucket, key):
        return validate_run_id(s3_utils.read_json(s3, bucket, key)["run_id"])
    run_id = new_run_id(now)
    s3_utils.put_json(s3, bucket, key, {"airflow_run_id": airflow_run_id, "run_id": run_id,
                                        "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return run_id


def ingestion_date(run_id: str) -> str:
    return f"{run_id[0:4]}-{run_id[4:6]}-{run_id[6:8]}"


def bronze_prefix(source_id: str, run_id: str) -> str:
    return f"bronze/source={source_id}/ingestion_date={ingestion_date(run_id)}/run_id={run_id}/"


def run_control_key(run_id: str, stage: str) -> str:
    return f"control/runs/run_id={run_id}/{stage}.json"


def _ctx(run_id: str, source_id: str = "-") -> str:
    """Prefijo de log con lote y fuente (requisito: logs con fuente, lote y tarea)."""
    return f"[run_id={run_id}][source={source_id}]"


# --------------------------------------------------------------------------- fetch
@dataclass
class FetchResult:
    http_status: int | None
    body: bytes
    error: str | None


Fetcher = Callable[[str, int, str], FetchResult]


def http_fetch(url: str, timeout: int, user_agent: str) -> FetchResult:
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8",
            },
        )
    except requests.RequestException as exc:
        return FetchResult(None, b"", f"{type(exc).__name__}: {exc}")
    return FetchResult(resp.status_code, resp.content, None)


def classify(result: FetchResult) -> tuple[str, int | None, str | None]:
    """Devuelve (estado, cantidad_de_items, detalle_error)."""
    if result.error:
        return NETWORK_ERROR, None, result.error
    if result.http_status != 200:
        return HTTP_ERROR, None, f"HTTP {result.http_status}"
    if not result.body.strip():
        return EMPTY_RESPONSE, None, "Respuesta HTTP 200 con cuerpo vacío"
    try:
        # defusedxml bloquea ataques XML (entidades externas, "billion laughs").
        root = SafeET.fromstring(result.body)
    except (ParseError, DefusedXmlException) as exc:
        return MALFORMED_XML, None, f"XML malformado: {exc}"
    channel = root.find("channel") if root.tag == "rss" else None
    if channel is None:
        return MALFORMED_XML, None, f"No es RSS 2.0 (raíz <{root.tag}> sin <channel>)"
    n_items = len(channel.findall("item"))
    return (OK if n_items else OK_EMPTY), n_items, None


# --------------------------------------------------------------------------- ingest
def ingest_source(
    s3, settings: Settings, source: Source, run_id: str, fetcher: Fetcher = http_fetch
) -> dict:
    ctx = _ctx(run_id, source.source_id)
    prefix = bronze_prefix(source.source_id, run_id)
    manifest_key = prefix + "manifest.json"

    if s3_utils.object_exists(s3, settings.bucket, manifest_key):
        previous = s3_utils.read_json(s3, settings.bucket, manifest_key)
        if previous.get("status") in SUCCESS_STATUSES:
            log.info("%s ya existe en Bronze (%s); no se vuelve a consultar", ctx, previous["status"])
            return {**previous, "action": "SKIPPED_ALREADY_INGESTED"}
        log.warning("%s intento previo con estado %s; se reintenta", ctx, previous.get("status"))

    ingested_at = datetime.now(timezone.utc)
    result = fetcher(source.url, settings.http_timeout, settings.user_agent)
    status, item_count, error = classify(result)
    content_hash = hashlib.sha256(result.body).hexdigest() if result.body else None

    raw_key = None
    if status in SUCCESS_STATUSES:
        raw_key = prefix + "feed.xml"
        s3_utils.put_bytes(s3, settings.bucket, raw_key, result.body, "application/rss+xml")
    elif result.body:
        # Se guarda lo recibido para poder explicar el rechazo, fuera de Bronze.
        raw_key = (
            f"quarantine/bronze_rejected/source={source.source_id}/"
            f"ingestion_date={ingestion_date(run_id)}/run_id={run_id}/response.bin"
        )
        s3_utils.put_bytes(s3, settings.bucket, raw_key, result.body, "application/octet-stream")

    manifest = {
        "run_id": run_id,
        "source_id": source.source_id,
        "media": source.media,
        "section": source.section,
        "url": source.url,
        "ingested_at_utc": ingested_at.isoformat(),
        "http_status": result.http_status,
        "status": status,
        "error": error,
        "item_count": item_count,
        "content_sha256": content_hash,
        "content_bytes": len(result.body),
        "s3_raw_key": raw_key,
        "action": "INGESTED",
    }
    # Se escribe al final: si falla antes, no queda un lote "a medias" marcado como exitoso.
    s3_utils.put_json(s3, settings.bucket, manifest_key, manifest)

    if status in SUCCESS_STATUSES:
        log.info("%s %s: %s items, sha256=%s…", ctx, status, item_count, content_hash[:12])
    else:
        log.error("%s %s: %s", ctx, status, error)
    return manifest


def ingest_all(
    settings: Settings,
    run_id: str,
    s3=None,
    fetcher: Fetcher = http_fetch,
) -> dict:
    """Ingiere todas las fuentes de un lote y registra el resumen en control/."""
    validate_run_id(run_id)
    s3 = s3 or s3_utils.s3_client(settings.region)

    results = []
    for source in settings.sources:
        try:
            results.append(ingest_source(s3, settings, source, run_id, fetcher))
        except Exception as exc:  # error inesperado (p. ej. S3): no ocultarlo como "sin noticias"
            log.exception("%s error inesperado", _ctx(run_id, source.source_id))
            results.append({"source_id": source.source_id, "status": "UNEXPECTED_ERROR",
                            "error": str(exc), "run_id": run_id})

    ok = [r for r in results if r["status"] in SUCCESS_STATUSES]
    if len(ok) == len(results):
        run_status = RUN_COMPLETE
    elif ok:
        run_status = RUN_PARTIAL
    else:
        run_status = RUN_FAILED

    summary = {
        "run_id": run_id,
        "stage": "bronze",
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_status": run_status,
        "sources_total": len(results),
        "sources_ok": len(ok),
        "items_received": sum(r.get("item_count") or 0 for r in ok),
        "sources": results,
    }
    s3_utils.put_json(s3, settings.bucket, run_control_key(run_id, "bronze"), summary)
    log.info("%s Bronze %s (%s/%s fuentes, %s items)", _ctx(run_id), run_status,
             len(ok), len(results), summary["items_received"])
    return summary


if __name__ == "__main__":  # Prueba manual:  python -m datalake.ingest_bronze
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out = ingest_all(load_settings(), new_run_id())
    print(out["run_status"], out["items_received"], "items")
