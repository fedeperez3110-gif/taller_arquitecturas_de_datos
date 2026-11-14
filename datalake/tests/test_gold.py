"""Pruebas de Gold: métricas por noticias distintas, sin doble conteo al repetir lotes."""
import boto3
import pytest
from moto import mock_aws

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.enrich import enrich
from datalake.gold import GOLD_TABLES, build_gold
from datalake.silver import build_silver
from tests.test_enrich import FakeComprehend
from tests.test_silver import BUCKET, NOW, RUN1, RUN2, SETTINGS, ingest, item, rss


def table(s3, name):
    prefix = GOLD_TABLES[name][0]
    return pio.read_dataset(s3, BUCKET, prefix, GOLD_TABLES[name][1])


def run_all(s3, run_id, medellin, justicia):
    ingest(s3, run_id, medellin, justicia)
    build_silver(SETTINGS, run_id, s3=s3, now=NOW)
    enrich(SETTINGS, run_id, s3=s3, comprehend=FakeComprehend(), now=NOW)
    return build_gold(SETTINGS, run_id, s3=s3, now=NOW)


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def batch():
    shared = item(9, title="Petro y Petro en Medellín")      # en ambos feeds; Petro 2 veces
    return (rss(item(1, title="Petro habla"), item(2, title="Ecopetrol sube"), shared),
            rss(item(3, title="Juicio en Medellín", pub="2026-11-10T08:00:00-05:00"), shared))


def test_news_counts_by_day_and_section(s3):
    run_all(s3, RUN1, *batch())
    by_sec = {(r["published_date"], r["section"]): r["news_count"] for r in table(s3, "news_by_section")}
    assert by_sec == {("2026-11-11", "medellin"): 3, ("2026-11-11", "justicia"): 1,
                      ("2026-11-10", "justicia"): 1}
    by_day = {r["published_date"]: r["news_count"] for r in table(s3, "news_by_day")}
    assert by_day == {"2026-11-11": 3, "2026-11-10": 1}      # la compartida cuenta 1 vez en el total


def test_entity_presence_counts_distinct_news_not_mentions(s3):
    run_all(s3, RUN1, *batch())
    ents = {(r["published_date"], r["entity_normalized"]): r for r in table(s3, "entity_daily")}
    petro = ents[("2026-11-11", "petro")]
    assert petro["news_count"] == 2              # 2 noticias distintas
    assert petro["mention_count"] == 3           # 3 menciones (no se usa como presencia)
    assert ents[("2026-11-11", "medellin")]["news_count"] == 1
    assert ents[("2026-11-10", "medellin")]["news_count"] == 1   # P4: presencia por fecha


def test_phrases_by_section(s3):
    run_all(s3, RUN1, *batch())
    rows = {(r["published_date"], r["section"]): r["news_count"] for r in table(s3, "phrase_by_section")
            if r["phrase_normalized"] == "la reforma tributaria"}
    assert rows == {("2026-11-11", "medellin"): 3, ("2026-11-11", "justicia"): 1,
                    ("2026-11-10", "justicia"): 1}


def test_repeating_batches_never_double_counts(s3):
    run_all(s3, RUN1, *batch())
    before = {k: s3_utils.read_bytes(s3, BUCKET, k) for k in s3_utils.list_keys(s3, BUCKET, "gold/")}
    out_repeat = build_gold(SETTINGS, RUN1, s3=s3, now=NOW)            # repetir el mismo lote
    out_new = run_all(s3, RUN2, *batch())                              # lote nuevo con las mismas noticias
    after = {k: s3_utils.read_bytes(s3, BUCKET, k) for k in s3_utils.list_keys(s3, BUCKET, "gold/")}
    assert before == after
    assert all(v == [] for v in out_repeat["partitions_written"].values())
    assert all(v == [] for v in out_new["partitions_written"].values())
