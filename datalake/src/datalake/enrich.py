"""Enriquecimiento con Amazon Comprehend (requisito 4.3).

Para cada noticia vigente de silver/news se analiza  título + descripción  (texto limpio, en español):
  * DetectEntities  -> PERSON, ORGANIZATION, LOCATION
  * DetectKeyPhrases

Caché por texto analizado:
  cache/comprehend/<hh>/<content_hash>.json
El content_hash es el hash de título+descripción limpios. Si la noticia no cambia, su hash
tampoco y NO se vuelve a llamar a Comprehend (ni en lotes nuevos ni en reprocesos).
Si el texto cambia (nueva versión en Silver), cambia el hash y se analiza de nuevo.
Si cambia la configuración del análisis (idioma, filtros), se sube ANALYSIS_VERSION y se reanaliza.

Salidas (un archivo por partición de fecha, igual que silver/news):
  silver/news_entities/published_date=<fecha>/part-00000.parquet
  silver/news_key_phrases/published_date=<fecha>/part-00000.parquet
Se reconstruyen a partir del conjunto vigente + la caché, por lo que nunca quedan entidades
de noticias que ya no existen o de versiones anteriores de una noticia.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.config import Settings, load_settings
from datalake.ingest_bronze import validate_run_id
from datalake.silver import load_current_news, partition_of
from datalake.text_utils import normalize_term, truncate_utf8

log = logging.getLogger(__name__)

LANGUAGE = "es"
ANALYSIS_VERSION = "v1"                     # súbelo si cambian idioma o reglas -> fuerza reanálisis
ENTITY_TYPES = {"PERSON", "ORGANIZATION", "LOCATION"}
MIN_SCORE = 0.50                            # confianza mínima para conservar una entidad/frase
MIN_PHRASE_CHARS = 3
BATCH_SIZE = 25                             # máximo de documentos por llamada Batch* de Comprehend
MAX_TEXT_BYTES = 4500                       # margen bajo el límite de 5 000 bytes por documento en Batch*

CACHE_PREFIX = "cache/comprehend/"
ENTITIES_PREFIX = "silver/news_entities/"
PHRASES_PREFIX = "silver/news_key_phrases/"


def _ctx(run_id: str) -> str:
    return f"[run_id={run_id}][task=enrich_comprehend]"


def comprehend_client(region: str):
    # Reintentos adaptativos: absorben throttling (límite de llamadas por segundo) sin fallar la tarea.
    return boto3.client("comprehend", region_name=region,
                        config=Config(retries={"mode": "adaptive", "max_attempts": 8}))


def analysis_text(news: dict) -> str:
    title, desc = news["title"], news.get("description") or ""
    return f"{title}\n{desc}" if desc else title


def cache_key(content_hash: str) -> str:
    return f"{CACHE_PREFIX}{content_hash[:2]}/{content_hash}.json"


# ============================================================================ caché
def load_cache(s3, bucket: str, content_hash: str) -> dict | None:
    key = cache_key(content_hash)
    if not s3_utils.object_exists(s3, bucket, key):
        return None
    entry = s3_utils.read_json(s3, bucket, key)
    if entry.get("analysis_version") != ANALYSIS_VERSION or entry.get("language") != LANGUAGE:
        return None  # resultado con otra configuración: se reanaliza
    return entry


# ============================================================================ llamadas a Comprehend
def analyze_texts(client, texts: dict[str, str]) -> tuple[dict[str, dict], dict[str, str]]:
    """Analiza {content_hash: texto}. Devuelve ({hash: resultado}, {hash: error})."""
    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    items = sorted(texts.items())
    for start in range(0, len(items), BATCH_SIZE):
        chunk = items[start:start + BATCH_SIZE]
        prepared = [truncate_utf8(text, MAX_TEXT_BYTES) for _, text in chunk]
        docs = [t for t, _ in prepared]

        ents = client.batch_detect_entities(TextList=docs, LanguageCode=LANGUAGE)
        phrs = client.batch_detect_key_phrases(TextList=docs, LanguageCode=LANGUAGE)
        failed = {e["Index"]: f'{e.get("ErrorCode")}: {e.get("ErrorMessage")}'
                  for e in ents.get("ErrorList", []) + phrs.get("ErrorList", [])}
        ent_by_idx = {r["Index"]: r["Entities"] for r in ents.get("ResultList", [])}
        phr_by_idx = {r["Index"]: r["KeyPhrases"] for r in phrs.get("ResultList", [])}

        for idx, (content_hash, _) in enumerate(chunk):
            if idx in failed or idx not in ent_by_idx or idx not in phr_by_idx:
                errors[content_hash] = failed.get(idx, "Resultado incompleto de Comprehend")
                continue
            results[content_hash] = {
                "entities": ent_by_idx[idx],
                "key_phrases": phr_by_idx[idx],
                "text_bytes": len(docs[idx].encode("utf-8")),
                "truncated": prepared[idx][1],
            }
    return results, errors


# ============================================================================ tablas derivadas
def entity_rows(news: dict, cache: dict) -> list[dict]:
    """Agrupa las menciones por (tipo, entidad normalizada): 1 fila por noticia y entidad."""
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for ent in cache["entities"]:
        if ent["Type"] not in ENTITY_TYPES or ent["Score"] < MIN_SCORE:
            continue
        norm = normalize_term(ent["Text"])
        if norm:
            groups[(ent["Type"], norm)].append(ent)
    rows = []
    for (etype, norm), mentions in sorted(groups.items()):
        texts = [m["Text"].strip() for m in mentions]
        rows.append({
            "news_id": news["news_id"], "content_hash": news["content_hash"],
            "entity_type": etype,
            "entity_text": max(sorted(set(texts)), key=texts.count),   # forma más frecuente
            "entity_normalized": norm,
            "mention_count": len(mentions),
            "max_score": round(max(m["Score"] for m in mentions), 6),
            "media": news["media"], "sections": news["sections"],
            "published_at_utc": news["published_at_utc"],
        })
    return rows


def phrase_rows(news: dict, cache: dict) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for ph in cache["key_phrases"]:
        norm = normalize_term(ph["Text"])
        if ph["Score"] < MIN_SCORE or len(norm) < MIN_PHRASE_CHARS or norm.replace(" ", "").isdigit():
            continue
        groups[norm].append(ph)
    rows = []
    for norm, mentions in sorted(groups.items()):
        texts = [m["Text"].strip() for m in mentions]
        rows.append({
            "news_id": news["news_id"], "content_hash": news["content_hash"],
            "phrase_text": max(sorted(set(texts)), key=texts.count),
            "phrase_normalized": norm,
            "mention_count": len(mentions),
            "max_score": round(max(m["Score"] for m in mentions), 6),
            "media": news["media"], "sections": news["sections"],
            "published_at_utc": news["published_at_utc"],
        })
    return rows


# ============================================================================ orquestación
def enrich(settings: Settings, run_id: str, s3=None, comprehend=None,
           now: datetime | None = None) -> dict:
    validate_run_id(run_id)
    s3 = s3 or s3_utils.s3_client(settings.region)
    comprehend = comprehend or comprehend_client(settings.region)
    bucket = settings.bucket
    now = now or datetime.now(timezone.utc)

    news = load_current_news(s3, bucket)
    caches: dict[str, dict] = {}
    pending: dict[str, str] = {}
    for n in news.values():
        h = n["content_hash"]
        if h in caches or h in pending:
            continue
        entry = load_cache(s3, bucket, h)
        if entry is not None:
            caches[h] = entry
        else:
            pending[h] = analysis_text(n)

    log.info("%s noticias vigentes=%s textos distintos=%s en caché=%s por analizar=%s",
             _ctx(run_id), len(news), len(caches) + len(pending), len(caches), len(pending))

    results, errors = analyze_texts(comprehend, pending) if pending else ({}, {})
    for h, res in results.items():
        entry = {"content_hash": h, "language": LANGUAGE, "analysis_version": ANALYSIS_VERSION,
                 "analyzed_at_utc": now.isoformat(), "analyzed_in_run_id": run_id, **res}
        s3_utils.put_json(s3, bucket, cache_key(h), entry)
        caches[h] = entry
    for h, err in errors.items():
        log.error("%s Comprehend falló para content_hash=%s: %s (se reintentará en el próximo lote)",
                  _ctx(run_id), h[:12], err)

    ents_by_part: dict[str, list[dict]] = defaultdict(list)
    phr_by_part: dict[str, list[dict]] = defaultdict(list)
    without_entities = 0
    for n in news.values():
        cache = caches.get(n["content_hash"])
        if cache is None:
            continue  # pendiente por error: sin filas hasta que se analice
        part = partition_of(n)
        e_rows = entity_rows(n, cache)
        without_entities += not e_rows
        ents_by_part[part].extend(e_rows)
        phr_by_part[part].extend(phrase_rows(n, cache))

    ent_parts = pio.publish_partitioned(s3, bucket, ENTITIES_PREFIX, pio.ENTITY_SCHEMA, ents_by_part,
                                    lambda r: (r["news_id"], r["entity_type"], r["entity_normalized"]))
    phr_parts = pio.publish_partitioned(s3, bucket, PHRASES_PREFIX, pio.KEY_PHRASE_SCHEMA, phr_by_part,
                                    lambda r: (r["news_id"], r["phrase_normalized"]))

    summary = {
        "run_id": run_id, "stage": "comprehend", "processed_at_utc": now.isoformat(),
        "language": LANGUAGE, "analysis_version": ANALYSIS_VERSION,
        "news_current": len(news),
        "texts_from_cache": len(caches) - len(results),
        "texts_analyzed": len(results),             # llamadas efectivas (lo que se paga)
        "texts_failed": len(errors),
        "texts_truncated": sum(1 for r in results.values() if r["truncated"]),
        "news_without_entities": without_entities,
        "entity_rows": sum(len(v) for v in ents_by_part.values()),
        "key_phrase_rows": sum(len(v) for v in phr_by_part.values()),
        "entity_partitions_written": ent_parts,
        "key_phrase_partitions_written": phr_parts,
    }
    s3_utils.put_json(s3, bucket, f"control/runs/run_id={run_id}/comprehend.json", summary)
    log.info("%s analizados=%s desde_cache=%s fallidos=%s filas_entidades=%s filas_frases=%s "
             "particiones_entidades=%s particiones_frases=%s", _ctx(run_id), summary["texts_analyzed"],
             summary["texts_from_cache"], summary["texts_failed"], summary["entity_rows"],
             summary["key_phrase_rows"], ent_parts, phr_parts)
    return summary


if __name__ == "__main__":  # Prueba manual:  python -m datalake.enrich <run_id>
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(enrich(load_settings(), sys.argv[1]))
