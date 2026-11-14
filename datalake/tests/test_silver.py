"""Pruebas de la capa Silver con S3 simulado (moto). Cubren los escenarios de la sección 4.5."""
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.config import Settings, Source
from datalake.ingest_bronze import FetchResult, ingest_all
from datalake.silver import (
    BATCH_LOG_PREFIX, NEWS_PREFIX, QUARANTINE_PREFIX, build_silver, load_current_news,
)

BUCKET = "test-bucket"
NOW = datetime(2026, 11, 12, 3, 0, tzinfo=timezone.utc)
RUN1, RUN2, RUN3 = "20261112T010000Z", "20261112T020000Z", "20261112T030000Z"

SETTINGS = Settings(BUCKET, "us-east-2", "db", "wg", 5, "test", [
    Source("eltiempo_medellin", "el_tiempo", "medellin", "https://x.co/medellin.xml"),
    Source("eltiempo_justicia", "el_tiempo", "justicia", "https://x.co/justicia.xml"),
])


def item(n, title=None, desc="Descripción", pub="2026-11-11T06:00:00-05:00", link=None, guid="same"):
    link = link if link is not None else f"https://www.eltiempo.com/nota-{n}"
    guid_xml = "" if guid is None else f"<guid>{link if guid == 'same' else guid}</guid>"
    title = title if title is not None else f"Noticia {n}"
    pub_xml = "" if pub is None else f"<pubDate>{pub}</pubDate>"
    return (f"<item><title>{title}</title><link>{link}</link>{guid_xml}"
            f"<description>{desc}</description>{pub_xml}</item>")


def rss(*items):
    return ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>T</title>'
            + "".join(items) + "</channel></rss>").encode()


def ingest(s3, run_id, medellin: bytes | None, justicia: bytes | None):
    """Crea un lote en Bronze con el contenido indicado para cada fuente (None = fuente caída)."""
    def fetch(url, timeout, ua):
        body = medellin if "medellin" in url else justicia
        return FetchResult(200, body, None) if body is not None else FetchResult(503, b"", None)
    return ingest_all(SETTINGS, run_id, s3=s3, fetcher=fetch)


def news(s3):
    return load_current_news(s3, BUCKET)


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


# ------------------------------------------------------------------- carga inicial
def test_initial_load_cleans_dedups_and_reconciles(s3):
    shared = item(9, title="Compartida")
    ingest(s3, RUN1,
           rss(item(1, title="Alcald&#039;ía de <b>Medellín</b>"),              # etiqueta sin escapar
               item(2, title="<![CDATA[Gol de <i>Nacional</i> &amp; más]]>"),    # HTML dentro de CDATA
               shared),
           rss(item(3), shared))
    out = build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    t = out["totals"]

    # 5 ítems recibidos, 1 duplicado (la compartida), 4 noticias nuevas
    assert t["items_received"] == 5 and t["duplicates_in_batch"] == 1 and t["new"] == 4
    # Conciliación: recibidos = rechazados + duplicados + nuevos + actualizados + sin cambio + antiguos
    assert t["items_received"] == sum(t[k] for k in
                                      ("rejected", "duplicates_in_batch", "new", "updated", "unchanged", "stale"))

    rows = news(s3)
    assert len(rows) == 4
    by_title = {r["title"]: r for r in rows.values()}
    assert "Alcald'ía de Medellín" in by_title                               # HTML limpio
    assert "Gol de Nacional & más" in by_title
    assert by_title["Gol de Nacional & más"]["title_raw"] == "Gol de <i>Nacional</i> &amp; más"  # original conservado
    shared_row = by_title["Compartida"]
    assert shared_row["sections"] == ["justicia", "medellin"]                  # una fila, ambas secciones
    assert shared_row["published_at_utc"] == datetime(2026, 11, 11, 11, 0, tzinfo=timezone.utc)
    assert shared_row["published_at_raw"] == "2026-11-11T06:00:00-05:00"
    assert shared_row["published_date"] == "2026-11-11"
    assert shared_row["bronze_key"].startswith("bronze/source=") and shared_row["bronze_key"].endswith("feed.xml")
    assert s3_utils.list_keys(s3, BUCKET, NEWS_PREFIX) == [
        f"{NEWS_PREFIX}published_date=2026-11-11/part-00000.parquet"]


# ------------------------------------------------------------------- repetición de lote
def test_rerun_same_batch_is_idempotent(s3):
    ingest(s3, RUN1, rss(item(1), item(2)), rss(item(3)))
    build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    before = {k: s3_utils.read_bytes(s3, BUCKET, k) for k in s3_utils.list_keys(s3, BUCKET, NEWS_PREFIX)}

    out = build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    after = {k: s3_utils.read_bytes(s3, BUCKET, k) for k in s3_utils.list_keys(s3, BUCKET, NEWS_PREFIX)}

    assert out["totals"]["new"] == 0 and out["totals"]["unchanged"] == 3
    assert out["partitions_written"] == []          # nada cambió: no se reescribe ninguna partición
    assert before == after and len(news(s3)) == 3


# ------------------------------------------------------------------- lote incremental
def test_new_batch_adds_only_new_news(s3):
    ingest(s3, RUN1, rss(item(1), item(2)), rss(item(3)))
    build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    ingest(s3, RUN2, rss(item(1), item(2), item(4, pub="2026-11-12T01:00:00-05:00")), rss(item(3)))
    t = build_silver(SETTINGS, RUN2, s3=s3, now=NOW)["totals"]

    assert t["new"] == 1 and t["unchanged"] == 3
    rows = news(s3)
    assert len(rows) == 4
    old = next(r for r in rows.values() if r["title"] == "Noticia 1")
    assert old["first_seen_run_id"] == RUN1 and old["last_seen_run_id"] == RUN2 and old["version"] == 1


# ------------------------------------------------------------------- noticia modificada
def test_modified_news_creates_new_version_and_old_reprocess_is_stale(s3):
    ingest(s3, RUN1, rss(item(1, title="Título original")), rss())
    build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    ingest(s3, RUN2, rss(item(1, title="Título corregido")), rss())
    t2 = build_silver(SETTINGS, RUN2, s3=s3, now=NOW)["totals"]

    assert t2["updated"] == 1
    (row,) = news(s3).values()
    assert row["title"] == "Título corregido" and row["version"] == 2 and row["content_run_id"] == RUN2

    # Reprocesar el lote VIEJO no debe devolver la noticia a su versión anterior
    t1 = build_silver(SETTINGS, RUN1, s3=s3, now=NOW)["totals"]
    assert t1["stale"] == 1
    (row,) = news(s3).values()
    assert row["title"] == "Título corregido" and row["version"] == 2


def test_change_only_in_tracking_params_is_same_news(s3):
    ingest(s3, RUN1, rss(item(1, link="https://www.eltiempo.com/nota-1", guid=None)), rss())
    build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    ingest(s3, RUN2, rss(item(1, link="https://www.eltiempo.com/nota-1/?utm_source=rss", guid=None)), rss())
    t = build_silver(SETTINGS, RUN2, s3=s3, now=NOW)["totals"]
    assert t["unchanged"] == 1 and len(news(s3)) == 1


# ------------------------------------------------------------------- registros defectuosos
def test_invalid_records_go_to_quarantine_with_reason(s3):
    ingest(s3, RUN1,
           rss(item(1), item(2, title="  <b> </b> "), item(3, link="no-es-url", guid=None)),
           rss())
    t = build_silver(SETTINGS, RUN1, s3=s3, now=NOW)["totals"]
    assert t["rejected"] == 2 and t["new"] == 1

    (key,) = s3_utils.list_keys(s3, BUCKET, QUARANTINE_PREFIX)
    assert key == f"{QUARANTINE_PREFIX}run_id={RUN1}/part-00000.parquet"
    rejected = pio.read_table(s3, BUCKET, key).to_pylist()
    reasons = {r["item_position"]: r["reasons"] for r in rejected}
    assert reasons[1] == ["MISSING_TITLE"]
    assert set(reasons[2]) == {"MISSING_ID", "INVALID_LINK"}
    assert all(r["bronze_key"].endswith("feed.xml") and r["run_id"] == RUN1 for r in rejected)


def test_missing_or_invalid_date_goes_to_unknown_partition(s3):
    ingest(s3, RUN1, rss(item(1, pub=None), item(2, pub="mañana"), item(3)), rss())
    build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    rows = news(s3)
    status = {r["title"]: (r["pub_date_status"], r["published_date"]) for r in rows.values()}
    assert status["Noticia 1"] == ("MISSING", "unknown")
    assert status["Noticia 2"] == ("INVALID", "unknown")
    assert status["Noticia 3"] == ("OK", "2026-11-11")


# ------------------------------------------------------------------- fuente caída y registro por lote
def test_failed_source_is_logged_not_silently_empty(s3):
    ingest(s3, RUN1, rss(item(1)), None)          # Justicia no responde
    out = build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    by_src = {c["source_id"]: c for c in out["by_source"]}
    assert by_src["eltiempo_justicia"]["bronze_status"] == "HTTP_ERROR"
    assert by_src["eltiempo_justicia"]["items_received"] == 0
    log_rows = pio.read_table(s3, BUCKET, f"{BATCH_LOG_PREFIX}run_id={RUN1}/part-00000.parquet").to_pylist()
    assert {r["source_id"] for r in log_rows} == {"eltiempo_medellin", "eltiempo_justicia"}


def test_batch_without_bronze_fails_loudly(s3):
    with pytest.raises(RuntimeError, match="no hay datos en Bronze"):
        build_silver(SETTINGS, RUN3, s3=s3, now=NOW)


def test_news_moving_to_another_date_leaves_no_duplicate(s3):
    ingest(s3, RUN1, rss(item(1, pub="2026-11-10T10:00:00-05:00")), rss())
    build_silver(SETTINGS, RUN1, s3=s3, now=NOW)
    ingest(s3, RUN2, rss(item(1, title="Nuevo título", pub="2026-11-11T10:00:00-05:00")), rss())
    out = build_silver(SETTINGS, RUN2, s3=s3, now=NOW)
    assert out["partitions_written"] == ["2026-11-10", "2026-11-11"]
    assert s3_utils.list_keys(s3, BUCKET, NEWS_PREFIX) == [
        f"{NEWS_PREFIX}published_date=2026-11-11/part-00000.parquet"]   # la partición vieja quedó vacía y se borró
    assert len(news(s3)) == 1


def test_reprocessing_old_batch_does_not_regress_metadata_of_unchanged_news(s3):
    """Error detectado en AWS el 2026-11-13. Secuencia real: lote A (fecha P1), lote B ingerido después
    pero con la MISMA fecha P1 (copia controlada de A), lote C con la fecha corregida P2 y el mismo texto.
    Reprocesar B no debe devolver la fecha P1."""
    run_c = "20261112T040000Z"
    ingest(s3, RUN1, rss(item(1, pub="2026-11-11T06:00:00-05:00")), rss())
    ingest(s3, RUN2, rss(item(1, pub="2026-11-11T06:00:00-05:00")), rss())
    ingest(s3, run_c, rss(item(1, pub="2026-11-11T09:30:00-05:00")), rss())     # solo cambia la fecha
    for run in (RUN1, RUN2, run_c):
        build_silver(SETTINGS, run, s3=s3, now=NOW)
    (row,) = news(s3).values()
    assert row["published_at_raw"] == "2026-11-11T09:30:00-05:00" and row["version"] == 1

    out = build_silver(SETTINGS, RUN2, s3=s3, now=NOW)                          # reprocesar B
    (row,) = news(s3).values()
    assert row["published_at_raw"] == "2026-11-11T09:30:00-05:00"
    assert out["partitions_written"] == [] and out["totals"]["unchanged"] == 1


def test_intermediate_batch_cannot_impose_older_text(s3):
    """Lote A (texto T1), lote C posterior con el mismo T1, y luego se procesa un lote B intermedio con
    texto T2: como C ya mostró T1 después de B, la noticia debe seguir con T1 (B es 'stale')."""
    run_c = "20261112T040000Z"
    ingest(s3, RUN1, rss(item(1, title="T1")), rss())
    ingest(s3, RUN2, rss(item(1, title="T2")), rss())
    ingest(s3, run_c, rss(item(1, title="T1")), rss())
    for run in (RUN1, run_c, RUN2):                          # B se procesa al final
        out = build_silver(SETTINGS, run, s3=s3, now=NOW)
    (row,) = news(s3).values()
    assert row["title"] == "T1" and row["version"] == 1 and out["totals"]["stale"] == 1
