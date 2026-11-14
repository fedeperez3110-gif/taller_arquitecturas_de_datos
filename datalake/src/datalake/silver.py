"""Capa Silver: de los XML de Bronze a un conjunto VIGENTE de noticias (requisito 4.3).

Flujo para un lote (run_id):
  1. Leer los feed.xml válidos del lote en Bronze (nunca se consultan los RSS).
  2. Extraer cada <item>, limpiar HTML, normalizar fechas a UTC (conservando el original).
  3. Validar campos obligatorios; los inválidos van a cuarentena con su motivo.
  4. Calcular el identificador de la noticia y eliminar duplicados dentro del lote
     (la misma noticia puede venir en varios feeds -> una fila, con todas sus secciones).
  5. Comparar contra el conjunto vigente de Silver y clasificar cada noticia:
       NEW        no existía
       UNCHANGED  ya existía con el mismo texto (solo se actualizan secciones / last_seen)
       UPDATED    el texto (título o descripción) cambió y el lote es más reciente -> version+1
       STALE      el lote es MÁS ANTIGUO que el contenido vigente (p. ej. reprocesar un lote viejo):
                  no se retrocede el contenido
  6. Reescribir solo las particiones que cambiaron (un archivo por partición, reemplazo atómico).
  7. Registrar los conteos del lote (control/batch_log y control/runs/.../silver.json).

Conciliación por fuente:
  items_received = rejected + duplicates_in_batch + new + updated + unchanged + stale

Identificador de la noticia:
  news_id = sha256(medio | valor)[:32], donde valor es el GUID (normalizado si es URL)
  o, si no hay GUID, el enlace normalizado (sin utm_*, sin #fragmento, sin barra final).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from xml.etree.ElementTree import Element

from defusedxml import ElementTree as SafeET

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.config import Settings, load_settings
from datalake.ingest_bronze import SUCCESS_STATUSES, ingestion_date, validate_run_id
from datalake.text_utils import (
    clean_html, is_valid_http_url, normalize_url, parse_pub_date, sha256_text,
)

log = logging.getLogger(__name__)

NEWS_PREFIX = "silver/news/"
QUARANTINE_PREFIX = "quarantine/silver_rejected/"
BATCH_LOG_PREFIX = "control/batch_log/"

NEW, UNCHANGED, UPDATED, STALE = "new", "unchanged", "updated", "stale"

DC_NS = "{http://purl.org/dc/elements/1.1/}"

# Motivos de rechazo
R_MISSING_ID = "MISSING_ID"          # sin GUID ni enlace válido: no se puede identificar
R_MISSING_TITLE = "MISSING_TITLE"    # título vacío después de limpiar el HTML
R_INVALID_LINK = "INVALID_LINK"      # enlace ausente o que no es http(s)


def _ctx(run_id: str, source_id: str = "-") -> str:
    return f"[run_id={run_id}][source={source_id}][task=build_silver]"


# ============================================================================ 1-2. leer y parsear
def bronze_manifests(s3, bucket: str, run_id: str) -> list[dict]:
    """Manifiestos de todas las fuentes que tienen datos para ese lote en Bronze."""
    out = []
    for source_prefix in s3_utils.list_prefixes(s3, bucket, "bronze/"):
        key = f"{source_prefix}ingestion_date={ingestion_date(run_id)}/run_id={run_id}/manifest.json"
        if s3_utils.object_exists(s3, bucket, key):
            out.append(s3_utils.read_json(s3, bucket, key))
    return sorted(out, key=lambda m: m["source_id"])


def _text(item: Element, tag: str) -> str | None:
    el = item.find(tag)
    if el is None:
        return None
    # itertext() conserva el texto aunque el feed traiga etiquetas HTML sin escapar dentro del campo.
    text = "".join(el.itertext()).strip()
    return text or None


def parse_items(xml_bytes: bytes) -> list[dict]:
    """Extrae los campos disponibles de cada <item> de un RSS 2.0."""
    root = SafeET.fromstring(xml_bytes)
    channel = root.find("channel")
    items = []
    for pos, item in enumerate(channel.findall("item") if channel is not None else []):
        items.append({
            "item_position": pos,
            "guid": _text(item, "guid"),
            "link": _text(item, "link"),
            "title_raw": _text(item, "title"),
            "description_raw": _text(item, "description"),
            "published_at_raw": _text(item, "pubDate") or _text(item, f"{DC_NS}date"),
        })
    return items


# ============================================================================ 3-4. validar e identificar
def identify(guid: str | None, link: str | None, media: str) -> tuple[str, str, str] | None:
    """Devuelve (news_id, estrategia, valor_normalizado) o None si no se puede identificar."""
    if guid:
        value = normalize_url(guid) if is_valid_http_url(guid) else guid.strip()
        strategy = "GUID"
    elif is_valid_http_url(link):
        value = normalize_url(link)
        strategy = "LINK"
    else:
        return None
    return sha256_text(media, value)[:32], strategy, value


@dataclass
class Candidate:
    """Una noticia válida leída del lote (antes de compararla con Silver)."""
    row: dict
    source_id: str
    occurrences: int = 1
    sections: set[str] = field(default_factory=set)
    source_ids: set[str] = field(default_factory=set)


def build_candidate(raw: dict, manifest: dict, run_id: str, now: datetime
                    ) -> tuple[dict | None, list[str]]:
    title = clean_html(raw["title_raw"])
    description = clean_html(raw["description_raw"])
    reasons = []
    ident = identify(raw["guid"], raw["link"], manifest["media"])
    if ident is None:
        reasons.append(R_MISSING_ID)
    if not title:
        reasons.append(R_MISSING_TITLE)
    if not is_valid_http_url(raw["link"]):
        reasons.append(R_INVALID_LINK)
    if reasons:
        return None, reasons

    news_id, strategy, id_value = ident
    published_utc, date_status = parse_pub_date(raw["published_at_raw"], now=now)
    # Se trunca a milisegundos, la misma precisión con que se guarda en Parquet; así,
    # reprocesar el mismo lote compara valores idénticos (idempotencia).
    ingested_at = datetime.fromisoformat(manifest["ingested_at_utc"])
    ingested_at = ingested_at.replace(microsecond=ingested_at.microsecond // 1000 * 1000)
    row = {
        "news_id": news_id,
        "id_strategy": strategy,
        "id_source_value": id_value,
        "guid": raw["guid"],
        "link": raw["link"].strip(),
        "title": title,
        "description": description,
        "title_raw": raw["title_raw"],
        "description_raw": raw["description_raw"],
        "published_at_raw": raw["published_at_raw"],
        "published_at_utc": published_utc,
        "pub_date_status": date_status,
        "media": manifest["media"],
        "sections": [manifest["section"]],
        "source_ids": [manifest["source_id"]],
        "content_hash": sha256_text(title, description),
        "version": 1,
        "first_seen_run_id": run_id,
        "first_seen_at_utc": ingested_at,
        "last_seen_run_id": run_id,
        "last_seen_at_utc": ingested_at,
        "content_run_id": run_id,
        "content_ingested_at_utc": ingested_at,
        "bronze_key": manifest["s3_raw_key"],
    }
    return row, []


def partition_of(row: dict) -> str:
    ts = row.get("published_at_utc")
    return ts.strftime("%Y-%m-%d") if ts else pio.UNKNOWN_DATE


# ============================================================================ 5. comparar con Silver
def merge_news(current: dict[str, dict], incoming: dict[str, dict]
               ) -> tuple[dict[str, dict], dict[str, str]]:
    """Combina el conjunto vigente con las noticias del lote.

    Devuelve (nuevo_conjunto_vigente, clasificación por news_id). Es determinista:
    aplicar dos veces el mismo lote da el mismo resultado (idempotencia).
    """
    result = dict(current)
    status: dict[str, str] = {}
    for nid, inc in incoming.items():
        cur = current.get(nid)
        if cur is None:
            result[nid] = inc
            status[nid] = NEW
            continue

        # "Más reciente" = posterior a la ÚLTIMA vez que se vio la noticia (en cualquier lote), no a la
        # fecha de su contenido vigente. Si no, reprocesar un lote intermedio podría imponer una fecha o
        # un texto más viejos que lo que el medio publicó después (error detectado en AWS el 2026-11-13).
        newer = inc["last_seen_at_utc"] > cur["last_seen_at_utc"]
        same_text = inc["content_hash"] == cur["content_hash"]
        same_batch = inc["content_run_id"] == cur["content_run_id"]

        merged = dict(cur)
        merged["sections"] = sorted(set(cur["sections"]) | set(inc["sections"]))
        merged["source_ids"] = sorted(set(cur["source_ids"]) | set(inc["source_ids"]))
        if inc["first_seen_at_utc"] < cur["first_seen_at_utc"]:
            merged["first_seen_run_id"] = inc["first_seen_run_id"]
            merged["first_seen_at_utc"] = inc["first_seen_at_utc"]
        if newer:
            merged["last_seen_run_id"] = inc["last_seen_run_id"]
            merged["last_seen_at_utc"] = inc["last_seen_at_utc"]

        if same_text:
            status[nid] = UNCHANGED
            if newer:  # p. ej. el medio corrigió solo la fecha: se toman los metadatos más recientes
                for k in ("published_at_raw", "published_at_utc", "pub_date_status", "link", "guid"):
                    merged[k] = inc[k]
        elif newer or same_batch:
            # Texto distinto y lote más reciente -> nueva versión vigente.
            # (same_batch: reproceso del mismo lote con reglas de limpieza nuevas; no sube la versión)
            content_keys = ("guid", "link", "title", "description", "title_raw", "description_raw",
                            "published_at_raw", "published_at_utc", "pub_date_status",
                            "content_hash", "content_run_id", "content_ingested_at_utc", "bronze_key")
            for k in content_keys:
                merged[k] = inc[k]
            merged["version"] = cur["version"] if same_batch else cur["version"] + 1
            status[nid] = UPDATED
        else:
            status[nid] = STALE  # lote viejo: no se retrocede el contenido vigente
        result[nid] = merged
    return result, status


# ============================================================================ 6. publicar
def _comparable(row: dict) -> dict:
    return {k: row.get(k) for k in pio.NEWS_SCHEMA.names}


def publish_news(s3, bucket: str, before: dict[str, dict], after: dict[str, dict]) -> list[str]:
    """Reescribe solo las particiones afectadas. Devuelve las particiones tocadas."""
    touched: set[str] = set()
    for nid, row in after.items():
        old = before.get(nid)
        if old is None or _comparable(old) != _comparable(row) or partition_of(old) != partition_of(row):
            touched.add(partition_of(row))
            if old is not None:
                touched.add(partition_of(old))   # si cambió de fecha, la partición vieja también cambia

    by_partition: dict[str, list[dict]] = defaultdict(list)
    for row in after.values():
        by_partition[partition_of(row)].append(row)

    for part in sorted(touched):
        key = f"{NEWS_PREFIX}published_date={part}/{pio.PART_FILE}"
        rows = sorted(by_partition.get(part, []), key=lambda r: r["news_id"])
        if rows:
            pio.write_table(s3, bucket, key, pio.table_from_rows([_comparable(r) for r in rows], pio.NEWS_SCHEMA))
        elif s3_utils.object_exists(s3, bucket, key):
            s3_utils.delete_key(s3, bucket, key)
    return sorted(touched)


def load_current_news(s3, bucket: str) -> dict[str, dict]:
    return {r["news_id"]: r for r in pio.read_dataset(s3, bucket, NEWS_PREFIX, pio.NEWS_SCHEMA)}


# ============================================================================ orquestación
def build_silver(settings: Settings, run_id: str, s3=None, now: datetime | None = None) -> dict:
    validate_run_id(run_id)
    s3 = s3 or s3_utils.s3_client(settings.region)
    bucket = settings.bucket
    now = now or datetime.now(timezone.utc)

    manifests = bronze_manifests(s3, bucket, run_id)
    if not manifests:
        raise RuntimeError(f"{_ctx(run_id)} no hay datos en Bronze para este lote")

    counts: dict[str, dict] = {}
    rejected_rows: list[dict] = []
    candidates: dict[str, Candidate] = {}

    for m in manifests:
        sid = m["source_id"]
        c = counts[sid] = {"run_id": run_id, "source_id": sid, "bronze_status": m["status"],
                           "items_received": 0, "rejected": 0, "duplicates_in_batch": 0,
                           NEW: 0, UPDATED: 0, UNCHANGED: 0, STALE: 0}
        if m["status"] not in SUCCESS_STATUSES:
            log.warning("%s sin datos en Bronze (estado %s); no aporta noticias a este lote",
                        _ctx(run_id, sid), m["status"])
            continue

        items = parse_items(s3_utils.read_bytes(s3, bucket, m["s3_raw_key"]))
        c["items_received"] = len(items)
        for raw in items:
            row, reasons = build_candidate(raw, m, run_id, now)
            if row is None:
                c["rejected"] += 1
                rejected_rows.append({
                    "run_id": run_id, "source_id": sid, "section": m["section"],
                    "bronze_key": m["s3_raw_key"], "reasons": reasons, "rejected_at_utc": now,
                    **{k: raw[k] for k in ("item_position", "guid", "link", "title_raw",
                                           "description_raw", "published_at_raw")},
                })
                continue
            nid = row["news_id"]
            if nid in candidates:          # misma noticia en otro feed o repetida en el mismo feed
                c["duplicates_in_batch"] += 1
                cand = candidates[nid]
                cand.occurrences += 1
                cand.sections.add(m["section"])
                cand.source_ids.add(sid)
            else:
                candidates[nid] = Candidate(row=row, source_id=sid,
                                            sections={m["section"]}, source_ids={sid})

    incoming = {}
    for nid, cand in candidates.items():
        row = dict(cand.row)
        row["sections"] = sorted(cand.sections)
        row["source_ids"] = sorted(cand.source_ids)
        incoming[nid] = row

    current = load_current_news(s3, bucket)
    merged, status = merge_news(current, incoming)
    for nid, st in status.items():
        counts[candidates[nid].source_id][st] += 1

    partitions = publish_news(s3, bucket, current, merged)

    # Cuarentena del lote: se reescribe completa (idempotente). Sin rechazos -> se borra si existía.
    q_key = f"{QUARANTINE_PREFIX}run_id={run_id}/{pio.PART_FILE}"
    if rejected_rows:
        pio.write_table(s3, bucket, q_key, pio.table_from_rows(rejected_rows, pio.QUARANTINE_SCHEMA))
    elif s3_utils.object_exists(s3, bucket, q_key):
        s3_utils.delete_key(s3, bucket, q_key)

    log_rows = [{**c, "processed_at_utc": now} for c in counts.values()]
    pio.write_table(s3, bucket, f"{BATCH_LOG_PREFIX}run_id={run_id}/{pio.PART_FILE}",
                    pio.table_from_rows(log_rows, pio.BATCH_LOG_SCHEMA))

    totals = {k: sum(c[k] for c in counts.values())
              for k in ("items_received", "rejected", "duplicates_in_batch", NEW, UPDATED, UNCHANGED, STALE)}
    summary = {
        "run_id": run_id,
        "stage": "silver",
        "processed_at_utc": now.isoformat(),
        "totals": totals,
        "news_in_batch": len(incoming),
        "news_current_total": len(merged),
        "partitions_written": partitions,
        "by_source": list(counts.values()),
    }
    s3_utils.put_json(s3, bucket, f"control/runs/run_id={run_id}/silver.json", summary)

    for c in counts.values():
        log.info("%s recibidos=%s rechazados=%s duplicados=%s nuevos=%s actualizados=%s "
                 "sin_cambio=%s antiguos=%s", _ctx(run_id, c["source_id"]), c["items_received"],
                 c["rejected"], c["duplicates_in_batch"], c[NEW], c[UPDATED], c[UNCHANGED], c[STALE])
    log.info("%s Silver listo: %s noticias únicas en el lote, %s vigentes en total, particiones=%s",
             _ctx(run_id), len(incoming), len(merged), partitions)
    return summary


if __name__ == "__main__":  # Prueba manual:  python -m datalake.silver <run_id>
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(build_silver(load_settings(), sys.argv[1])["totals"])
