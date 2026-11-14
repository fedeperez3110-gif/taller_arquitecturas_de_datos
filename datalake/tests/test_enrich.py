"""Pruebas del enriquecimiento con un Comprehend simulado (no llama a AWS ni cuesta dinero)."""
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.enrich import CACHE_PREFIX, ENTITIES_PREFIX, PHRASES_PREFIX, enrich
from datalake.silver import build_silver, load_current_news
from tests.test_silver import BUCKET, NOW, RUN1, RUN2, SETTINGS, ingest, item, rss


class FakeComprehend:
    """Devuelve entidades según palabras del texto y registra cada texto analizado."""

    KNOWN = {"Petro": "PERSON", "Ecopetrol": "ORGANIZATION", "Medellín": "LOCATION",
             "lunes": "DATE"}  # DATE debe descartarse

    def __init__(self, fail_texts_containing: str | None = None):
        self.analyzed: list[str] = []
        self.fail = fail_texts_containing

    def _entities(self, text):
        out = []
        for word, etype in self.KNOWN.items():
            start = 0
            while (i := text.find(word, start)) >= 0:
                out.append({"Text": word, "Type": etype, "Score": 0.99, "BeginOffset": i,
                            "EndOffset": i + len(word)})
                start = i + 1
        return out

    def batch_detect_entities(self, TextList, LanguageCode):
        assert LanguageCode == "es" and len(TextList) <= 25
        self.analyzed.extend(TextList)
        res, err = [], []
        for i, t in enumerate(TextList):
            if self.fail and self.fail in t:
                err.append({"Index": i, "ErrorCode": "INTERNAL_SERVER_ERROR", "ErrorMessage": "boom"})
            else:
                res.append({"Index": i, "Entities": self._entities(t)})
        return {"ResultList": res, "ErrorList": err}

    def batch_detect_key_phrases(self, TextList, LanguageCode):
        return {"ResultList": [{"Index": i, "KeyPhrases": [
            {"Text": "la reforma tributaria", "Score": 0.95},
            {"Text": "La Reforma Tributaria", "Score": 0.90},   # repetida -> 1 fila, mention_count=2
            {"Text": "2026", "Score": 0.99},                    # solo números -> se descarta
            {"Text": "algo dudoso", "Score": 0.10},             # baja confianza -> se descarta
        ]} for i in range(len(TextList))], "ErrorList": []}


def rows(s3, prefix):
    out = []
    for k in s3_utils.list_keys(s3, BUCKET, prefix):
        out.extend(pio.read_table(s3, BUCKET, k).to_pylist())
    return out


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def load(s3, run_id, *items_medellin):
    ingest(s3, run_id, rss(*items_medellin), rss())
    build_silver(SETTINGS, run_id, s3=s3, now=NOW)


def test_entities_are_one_row_per_news_and_entity(s3):
    load(s3, RUN1, item(1, title="Petro visitó Medellín", desc="Petro habló con Ecopetrol el lunes"))
    fake = FakeComprehend()
    out = enrich(SETTINGS, RUN1, s3=s3, comprehend=fake, now=NOW)

    ents = {(r["entity_type"], r["entity_normalized"]): r for r in rows(s3, ENTITIES_PREFIX)}
    assert set(ents) == {("PERSON", "petro"), ("LOCATION", "medellin"), ("ORGANIZATION", "ecopetrol")}
    assert ents[("PERSON", "petro")]["mention_count"] == 2           # 2 menciones, 1 sola fila
    assert ents[("LOCATION", "medellin")]["entity_text"] == "Medellín"
    (phrase,) = rows(s3, PHRASES_PREFIX)
    assert phrase["phrase_normalized"] == "la reforma tributaria" and phrase["mention_count"] == 2
    assert out["texts_analyzed"] == 1 and out["entity_rows"] == 3


def test_unchanged_text_uses_cache_and_does_not_call_comprehend(s3):
    load(s3, RUN1, item(1, title="Petro en Medellín"), item(2, title="Ecopetrol"))
    enrich(SETTINGS, RUN1, s3=s3, comprehend=FakeComprehend(), now=NOW)
    files_before = {k: s3_utils.read_bytes(s3, BUCKET, k) for k in s3_utils.list_keys(s3, BUCKET, "silver/news_")}

    load(s3, RUN2, item(1, title="Petro en Medellín"), item(2, title="Ecopetrol"))   # lote nuevo, mismas noticias
    fake = FakeComprehend()
    out = enrich(SETTINGS, RUN2, s3=s3, comprehend=fake, now=NOW)

    assert fake.analyzed == [] and out["texts_analyzed"] == 0 and out["texts_from_cache"] == 2
    assert out["entity_partitions_written"] == [] and out["key_phrase_partitions_written"] == []
    assert files_before == {k: s3_utils.read_bytes(s3, BUCKET, k) for k in s3_utils.list_keys(s3, BUCKET, "silver/news_")}


def test_modified_text_is_reanalyzed_and_old_entities_disappear(s3):
    load(s3, RUN1, item(1, title="Petro en Medellín"), item(2, title="Ecopetrol"))
    enrich(SETTINGS, RUN1, s3=s3, comprehend=FakeComprehend(), now=NOW)

    load(s3, RUN2, item(1, title="Ecopetrol en Medellín"), item(2, title="Ecopetrol"))  # cambia solo la 1
    fake = FakeComprehend()
    out = enrich(SETTINGS, RUN2, s3=s3, comprehend=fake, now=NOW)

    assert fake.analyzed == ["Ecopetrol en Medellín\nDescripción"] and out["texts_analyzed"] == 1
    news = load_current_news(s3, BUCKET)
    ents = rows(s3, ENTITIES_PREFIX)
    assert ("PERSON", "petro") not in {(r["entity_type"], r["entity_normalized"]) for r in ents}
    # Ninguna entidad apunta a un texto que no sea el vigente de su noticia
    assert all(r["content_hash"] == news[r["news_id"]]["content_hash"] for r in ents)


def test_comprehend_error_is_not_cached_and_retried_next_time(s3):
    load(s3, RUN1, item(1, title="Petro falla"), item(2, title="Ecopetrol"))
    out = enrich(SETTINGS, RUN1, s3=s3, comprehend=FakeComprehend(fail_texts_containing="falla"), now=NOW)
    assert out["texts_failed"] == 1 and out["texts_analyzed"] == 1
    assert len(s3_utils.list_keys(s3, BUCKET, CACHE_PREFIX)) == 1

    fake = FakeComprehend()
    out = enrich(SETTINGS, RUN1, s3=s3, comprehend=fake, now=NOW)
    assert fake.analyzed == ["Petro falla\nDescripción"] and out["texts_failed"] == 0


def test_no_entities_without_a_current_news(s3):
    load(s3, RUN1, item(1, title="Petro", pub="2026-11-10T10:00:00-05:00"))
    enrich(SETTINGS, RUN1, s3=s3, comprehend=FakeComprehend(), now=NOW)
    # La noticia cambia de texto y de fecha: la partición vieja de entidades debe desaparecer
    load(s3, RUN2, item(1, title="Ecopetrol", pub="2026-11-11T10:00:00-05:00"))
    enrich(SETTINGS, RUN2, s3=s3, comprehend=FakeComprehend(), now=NOW)

    keys = s3_utils.list_keys(s3, BUCKET, ENTITIES_PREFIX)
    assert keys == [f"{ENTITIES_PREFIX}published_date=2026-11-11/part-00000.parquet"]
    news_ids = set(load_current_news(s3, BUCKET))
    assert {r["news_id"] for r in rows(s3, ENTITIES_PREFIX)} <= news_ids


def test_long_text_is_truncated_safely(s3):
    load(s3, RUN1, item(1, title="Petro", desc="ñ" * 5000))   # 10 000 bytes en UTF-8
    fake = FakeComprehend()
    out = enrich(SETTINGS, RUN1, s3=s3, comprehend=fake, now=NOW)
    assert out["texts_truncated"] == 1
    assert all(len(t.encode("utf-8")) <= 4500 for t in fake.analyzed)
