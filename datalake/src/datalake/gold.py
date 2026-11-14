"""Capa Gold: agregados para responder las preguntas del negocio (requisito 4.4).

Tablas (Parquet, particionadas por published_date = fecha de publicación en UTC):
  gold/news_by_section/   P1  noticias por día, medio y sección
  gold/news_by_day/       P1  noticias distintas por día y medio (total sin repetir secciones)
  gold/entity_daily/      P2, P4  personas / organizaciones / lugares por día
  gold/phrase_by_section/ P3  frases clave por sección y día

Reglas:
  * Todas las métricas cuentan NOTICIAS DISTINTAS (news_id), nunca menciones, para que una
    entidad repetida en un mismo texto no infle su presencia.
  * Cada noticia pertenece a una sola fecha, así que sumar news_count entre días da noticias
    distintas en el rango (no hay doble conteo entre particiones).
  * Noticias sin fecha de publicación válida quedan en la partición published_date=unknown:
    se incluyen en los totales generales pero no en las series por día.
  * Gold se RECALCULA desde el conjunto vigente de Silver (no se suman lotes). Por eso repetir o
    reprocesar un lote nunca produce doble conteo. Solo se reescriben las particiones que cambian.
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.config import Settings, load_settings
from datalake.enrich import ENTITIES_PREFIX, PHRASES_PREFIX
from datalake.ingest_bronze import validate_run_id
from datalake.silver import load_current_news, partition_of

log = logging.getLogger(__name__)

GOLD_TABLES = {
    "news_by_section": ("gold/news_by_section/", pio.GOLD_NEWS_BY_SECTION_SCHEMA,
                        lambda r: (r["media"], r["section"])),
    "news_by_day": ("gold/news_by_day/", pio.GOLD_NEWS_BY_DAY_SCHEMA, lambda r: (r["media"],)),
    "entity_daily": ("gold/entity_daily/", pio.GOLD_ENTITY_DAILY_SCHEMA,
                     lambda r: (r["entity_type"], r["entity_normalized"])),
    "phrase_by_section": ("gold/phrase_by_section/", pio.GOLD_PHRASE_BY_SECTION_SCHEMA,
                          lambda r: (r["section"], r["phrase_normalized"])),
}


def _most_common(texts: Counter) -> str:
    # Desempate alfabético para que el resultado sea determinista.
    return sorted(texts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def compute_gold(news: dict[str, dict], entities: list[dict], phrases: list[dict]
                 ) -> dict[str, dict[str, list[dict]]]:
    """Devuelve {tabla: {partición: filas}} a partir de Silver vigente."""
    out: dict[str, dict[str, list[dict]]] = {t: defaultdict(list) for t in GOLD_TABLES}

    # ---- P1
    by_section: dict[tuple, set] = defaultdict(set)
    by_day: dict[tuple, set] = defaultdict(set)
    sections_by_day: dict[tuple, set] = defaultdict(set)
    for n in news.values():
        part = partition_of(n)
        by_day[(part, n["media"])].add(n["news_id"])
        for sec in n["sections"]:
            by_section[(part, n["media"], sec)].add(n["news_id"])
            sections_by_day[(part, n["media"])].add(sec)
    for (part, media, sec), ids in by_section.items():
        out["news_by_section"][part].append({"media": media, "section": sec, "news_count": len(ids)})
    for (part, media), ids in by_day.items():
        out["news_by_day"][part].append({"media": media, "news_count": len(ids),
                                         "sections_count": len(sections_by_day[(part, media)])})

    # Solo filas de la versión vigente de cada noticia (defensa extra de coherencia con Silver)
    def current(row):
        n = news.get(row["news_id"])
        return n is not None and n["content_hash"] == row["content_hash"]

    # ---- P2 / P4
    ent_news: dict[tuple, set] = defaultdict(set)
    ent_mentions: dict[tuple, int] = defaultdict(int)
    ent_texts: dict[tuple, Counter] = defaultdict(Counter)
    for e in filter(current, entities):
        key = (partition_of(news[e["news_id"]]), e["entity_type"], e["entity_normalized"])
        ent_news[key].add(e["news_id"])
        ent_mentions[key] += e["mention_count"]
        ent_texts[key][e["entity_text"]] += 1
    for (part, etype, norm), ids in ent_news.items():
        out["entity_daily"][part].append({
            "entity_type": etype, "entity_normalized": norm,
            "entity_text": _most_common(ent_texts[(part, etype, norm)]),
            "news_count": len(ids), "mention_count": ent_mentions[(part, etype, norm)],
        })

    # ---- P3
    ph_news: dict[tuple, set] = defaultdict(set)
    ph_texts: dict[tuple, Counter] = defaultdict(Counter)
    for p in filter(current, phrases):
        n = news[p["news_id"]]
        for sec in n["sections"]:
            key = (partition_of(n), sec, p["phrase_normalized"])
            ph_news[key].add(p["news_id"])
            ph_texts[key][p["phrase_text"]] += 1
    for (part, sec, norm), ids in ph_news.items():
        out["phrase_by_section"][part].append({
            "section": sec, "phrase_normalized": norm,
            "phrase_text": _most_common(ph_texts[(part, sec, norm)]), "news_count": len(ids),
        })
    return out


def build_gold(settings: Settings, run_id: str, s3=None, now: datetime | None = None) -> dict:
    validate_run_id(run_id)
    s3 = s3 or s3_utils.s3_client(settings.region)
    bucket = settings.bucket
    now = now or datetime.now(timezone.utc)

    news = load_current_news(s3, bucket)
    entities = pio.read_dataset(s3, bucket, ENTITIES_PREFIX, pio.ENTITY_SCHEMA)
    phrases = pio.read_dataset(s3, bucket, PHRASES_PREFIX, pio.KEY_PHRASE_SCHEMA)
    tables = compute_gold(news, entities, phrases)

    written = {}
    for name, (prefix, schema, sort_key) in GOLD_TABLES.items():
        written[name] = pio.publish_partitioned(s3, bucket, prefix, schema, tables[name], sort_key)

    summary = {
        "run_id": run_id, "stage": "gold", "processed_at_utc": now.isoformat(),
        "news_current": len(news),
        "rows": {name: sum(len(v) for v in tables[name].values()) for name in GOLD_TABLES},
        "partitions_written": written,
    }
    s3_utils.put_json(s3, bucket, f"control/runs/run_id={run_id}/gold.json", summary)
    log.info("[run_id=%s][task=build_gold] noticias=%s filas=%s particiones_escritas=%s",
             run_id, len(news), summary["rows"], written)
    return summary


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(build_gold(load_settings(), sys.argv[1]))
