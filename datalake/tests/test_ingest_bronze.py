"""Pruebas de la ingesta Bronze con S3 simulado (moto). No usan AWS real ni internet.

Ejecutar:  pip install -r requirements-dev.txt && pytest -q
"""
import boto3
import pytest
from moto import mock_aws

from datalake import s3_utils
from datalake.config import Settings, Source
from datalake.ingest_bronze import (
    FetchResult, HTTP_ERROR, MALFORMED_XML, NETWORK_ERROR, OK, OK_EMPTY,
    RUN_COMPLETE, RUN_FAILED, RUN_PARTIAL, bronze_prefix, classify, ingest_all,
)

BUCKET = "test-bucket"
RUN_ID = "20261022T113000Z"

RSS_OK = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Test</title>
<item><title>Noticia 1</title><link>https://x.co/1</link><guid>https://x.co/1</guid>
<pubDate>2026-10-22T06:11:08-05:00</pubDate><description>Uno</description></item>
<item><title>Noticia 2</title><link>https://x.co/2</link><guid>https://x.co/2</guid>
<pubDate>2026-10-22T07:00:00-05:00</pubDate><description>Dos</description></item>
</channel></rss>"""
RSS_EMPTY = b'<?xml version="1.0"?><rss version="2.0"><channel><title>T</title></channel></rss>'


def settings(n_sources=2):
    srcs = [Source(f"src{i}", "el_tiempo", f"sec{i}", f"https://x.co/{i}.xml") for i in range(n_sources)]
    return Settings(BUCKET, "us-east-1", "db", "primary", 5, "test", srcs)


class FakeFetcher:
    """Simula respuestas por URL y cuenta cuántas veces se consulta cada una."""
    def __init__(self, responses):
        self.responses, self.calls = responses, {}

    def __call__(self, url, timeout, ua):
        self.calls[url] = self.calls.get(url, 0) + 1
        resp = self.responses[url]
        return resp(self.calls[url]) if callable(resp) else resp


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


# ---------------------------------------------------------------- clasificación
@pytest.mark.parametrize("result,expected", [
    (FetchResult(200, RSS_OK, None), OK),
    (FetchResult(200, RSS_EMPTY, None), OK_EMPTY),
    (FetchResult(503, b"down", None), HTTP_ERROR),
    (FetchResult(None, b"", "Timeout"), NETWORK_ERROR),
    (FetchResult(200, b"<rss><channel><item>", None), MALFORMED_XML),
    (FetchResult(200, b"<html><body>directorio</body></html>", None), MALFORMED_XML),
    (FetchResult(200, b"   ", None), "EMPTY_RESPONSE"),
])
def test_classify(result, expected):
    assert classify(result)[0] == expected


def test_classify_blocks_xml_bomb():
    bomb = b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]><rss><channel>&a;</channel></rss>'
    assert classify(FetchResult(200, bomb, None))[0] == MALFORMED_XML


# ---------------------------------------------------------------- ingesta
def test_complete_batch_writes_raw_and_manifest(s3):
    st = settings()
    fetch = FakeFetcher({s.url: FetchResult(200, RSS_OK, None) for s in st.sources})
    out = ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)

    assert out["run_status"] == RUN_COMPLETE and out["items_received"] == 4
    prefix = bronze_prefix("src0", RUN_ID)
    assert prefix == "bronze/source=src0/ingestion_date=2026-10-22/run_id=20261022T113000Z/"
    assert s3_utils.read_bytes(s3, BUCKET, prefix + "feed.xml") == RSS_OK   # original intacto
    m = s3_utils.read_json(s3, BUCKET, prefix + "manifest.json")
    assert m["http_status"] == 200 and len(m["content_sha256"]) == 64 and m["item_count"] == 2
    assert s3_utils.object_exists(s3, BUCKET, "control/runs/run_id=20261022T113000Z/bronze.json")


def test_rerun_same_batch_does_not_refetch_or_duplicate(s3):
    st = settings()
    fetch = FakeFetcher({s.url: FetchResult(200, RSS_OK, None) for s in st.sources})
    ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    keys_before = s3_utils.list_keys(s3, BUCKET, "bronze/")

    out = ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    assert all(c == 1 for c in fetch.calls.values())             # no se volvió a consultar
    assert s3_utils.list_keys(s3, BUCKET, "bronze/") == keys_before  # ni una copia nueva
    assert {r["action"] for r in out["sources"]} == {"SKIPPED_ALREADY_INGESTED"}


def test_partial_then_retry_recovers_only_failed_source(s3):
    st = settings()
    fetch = FakeFetcher({
        st.sources[0].url: FetchResult(200, RSS_OK, None),
        # falla la primera vez, responde bien en el reintento
        st.sources[1].url: lambda n: FetchResult(503, b"", None) if n == 1 else FetchResult(200, RSS_OK, None),
    })
    first = ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    assert first["run_status"] == RUN_PARTIAL

    second = ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    assert second["run_status"] == RUN_COMPLETE
    assert fetch.calls == {st.sources[0].url: 1, st.sources[1].url: 2}


def test_unavailable_source_is_not_reported_as_empty(s3):
    st = settings(1)
    fetch = FakeFetcher({st.sources[0].url: FetchResult(None, b"", "ConnectTimeout")})
    out = ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    assert out["run_status"] == RUN_FAILED
    assert out["sources"][0]["status"] == NETWORK_ERROR and out["items_received"] == 0
    assert not s3_utils.object_exists(s3, BUCKET, bronze_prefix("src0", RUN_ID) + "feed.xml")


def test_malformed_xml_goes_to_quarantine_not_bronze(s3):
    st = settings(1)
    fetch = FakeFetcher({st.sources[0].url: FetchResult(200, b"<rss><channel>", None)})
    out = ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    m = out["sources"][0]
    assert m["status"] == MALFORMED_XML
    assert m["s3_raw_key"].startswith("quarantine/bronze_rejected/source=src0/")
    assert not s3_utils.list_keys(s3, BUCKET, bronze_prefix("src0", RUN_ID) + "feed")


def test_new_batch_does_not_touch_previous_batch(s3):
    st = settings(1)
    fetch = FakeFetcher({st.sources[0].url: FetchResult(200, RSS_OK, None)})
    ingest_all(st, RUN_ID, s3=s3, fetcher=fetch)
    ingest_all(st, "20261022T133000Z", s3=s3, fetcher=fetch)
    feeds = [k for k in s3_utils.list_keys(s3, BUCKET, "bronze/") if k.endswith("feed.xml")]
    assert len(feeds) == 2


def test_invalid_run_id_rejected(s3):
    with pytest.raises(ValueError):
        ingest_all(settings(), "2026-10-22", s3=s3, fetcher=FakeFetcher({}))


def test_batch_run_id_uses_real_time_and_is_stable_across_retries(s3):
    from datetime import datetime, timezone
    from datalake.ingest_bronze import resolve_batch_run_id
    late = datetime(2026, 11, 13, 20, 25, 0, tzinfo=timezone.utc)   # ejecución programada que corre tarde
    airflow_run = "scheduled__2026-11-12T06:00:00+00:00"
    first = resolve_batch_run_id(s3, BUCKET, airflow_run, now=late)
    assert first == "20261113T202500Z"                              # hora real, no la fecha lógica
    again = resolve_batch_run_id(s3, BUCKET, airflow_run, now=datetime.now(timezone.utc))
    assert again == first                                           # reintento: mismo lote
    other = resolve_batch_run_id(s3, BUCKET, "manual__2026-11-13T21:00:00+00:00",
                                 now=datetime(2026, 11, 13, 21, 0, tzinfo=timezone.utc))
    assert other != first
