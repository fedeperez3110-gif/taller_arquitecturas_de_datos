"""Escenarios 4.5 de punta a punta con un lote controlado: modificado, defectuosos, nueva partición."""
from datetime import datetime, timedelta, timezone

import boto3
import pytest
from moto import mock_aws

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.controlled_batch import MODIFIED_SUFFIX, create_controlled_batch
from datalake.enrich import enrich
from datalake.gold import build_gold
from datalake.silver import build_silver, load_current_news
from tests.test_enrich import FakeComprehend
from tests.test_silver import BUCKET, NOW, SETTINGS, ingest, item, rss

# El lote de origen se ingiere con la hora REAL (ingest_all usa datetime.now), así que el lote
# controlado debe ser posterior a la hora real para que la noticia modificada cuente como "updated".
LATER = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(microsecond=0)


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_controlled_batch_end_to_end(s3):
    run1 = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y%m%dT%H%M%SZ")
    ingest(s3, run1, rss(item(1, title="Petro"), item(2, title="Ecopetrol"), item(3)), rss(item(4)))
    build_silver(SETTINGS, run1, s3=s3, now=NOW)
    enrich(SETTINGS, run1, s3=s3, comprehend=FakeComprehend(), now=NOW)
    build_gold(SETTINGS, run1, s3=s3, now=NOW)

    run = create_controlled_batch(SETTINGS, run1, modify=1, defects=True, s3=s3, now=LATER)
    assert run == LATER.strftime("%Y%m%dT%H%M%SZ")
    manifests = [k for k in s3_utils.list_keys(s3, BUCKET, "bronze/") if run in k and k.endswith("manifest.json")]
    assert len(manifests) == 2
    assert all(s3_utils.read_json(s3, BUCKET, k)["controlled_copy_of"] == run1 for k in manifests)

    t = build_silver(SETTINGS, run, s3=s3, now=LATER)["totals"]
    # medellin: 3 originales (1 modificada) + 3 defectuosos; justicia: 1 sin cambio
    assert t == {"items_received": 7, "rejected": 2, "duplicates_in_batch": 0,
                 "new": 1, "updated": 1, "unchanged": 3, "stale": 0}

    news = load_current_news(s3, BUCKET)
    modified = [n for n in news.values() if n["title"].endswith(MODIFIED_SUFFIX.strip())]
    assert len(modified) == 1 and modified[0]["version"] == 2 and modified[0]["content_run_id"] == run
    assert any(n["published_date"] == "unknown" for n in news.values())          # nueva partición

    fake = FakeComprehend()
    out = enrich(SETTINGS, run, s3=s3, comprehend=fake, now=LATER)
    assert out["texts_analyzed"] == 2           # solo la modificada y la nueva (fecha inválida)

    q = pio.read_table(s3, BUCKET, f"quarantine/silver_rejected/run_id={run}/part-00000.parquet").to_pylist()
    assert sorted(tuple(sorted(r["reasons"])) for r in q) == [("INVALID_LINK", "MISSING_ID"), ("MISSING_TITLE",)]
    assert all(r["bronze_key"].startswith("bronze/") and run in r["bronze_key"] for r in q)
