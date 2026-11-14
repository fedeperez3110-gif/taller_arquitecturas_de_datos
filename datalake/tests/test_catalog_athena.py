"""Registro de particiones en Glue (moto) y ejecutor de validaciones de Athena (cliente simulado)."""
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from datalake import s3_utils
from datalake.athena import VALIDATIONS_DIR, ValidationFailed, run_validations
from datalake.catalog import sync_all
from tests.test_silver import BUCKET, RUN1, SETTINGS

DB = SETTINGS.glue_database


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        glue = boto3.client("glue", region_name="us-east-2")
        glue.create_database(DatabaseInput={"Name": DB})
        glue.create_table(DatabaseName=DB, TableInput={
            "Name": "gold_news_by_day",
            "PartitionKeys": [{"Name": "published_date", "Type": "string"}],
            "StorageDescriptor": {
                "Location": f"s3://{BUCKET}/gold/news_by_day/",
                "Columns": [{"Name": "media", "Type": "string"}],
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        })
        yield s3, glue


def put(s3, day):
    s3_utils.put_bytes(s3, BUCKET, f"gold/news_by_day/published_date={day}/part-00000.parquet", b"x", "x")


def glue_values(glue):
    parts = glue.get_partitions(DatabaseName=DB, TableName="gold_news_by_day")["Partitions"]
    return {p["Values"][0]: p["StorageDescriptor"]["Location"] for p in parts}


def test_new_partitions_are_registered_and_empty_ones_removed(aws):
    s3, glue = aws
    put(s3, "2026-11-10"); put(s3, "2026-11-11")
    (r,) = sync_all(SETTINGS, RUN1, s3=s3, glue=glue)
    assert r["added"] == ["2026-11-10", "2026-11-11"]
    assert glue_values(glue)["2026-11-11"] == f"s3://{BUCKET}/gold/news_by_day/published_date=2026-11-11/"

    # Nueva partición (día nuevo) + una que quedó vacía
    put(s3, "2026-11-12")
    s3_utils.delete_key(s3, BUCKET, "gold/news_by_day/published_date=2026-11-10/part-00000.parquet")
    (r,) = sync_all(SETTINGS, RUN1, s3=s3, glue=glue)
    assert r["added"] == ["2026-11-12"] and r["removed"] == ["2026-11-10"]
    assert set(glue_values(glue)) == {"2026-11-11", "2026-11-12"}

    (r,) = sync_all(SETTINGS, RUN1, s3=s3, glue=glue)       # idempotente
    assert r["added"] == [] and r["removed"] == []


class FakeAthena:
    def __init__(self, violations_by_marker: dict[str, int]):
        self.violations = violations_by_marker
        self.queries: list[str] = []

    def start_query_execution(self, QueryString, QueryExecutionContext, WorkGroup):
        assert QueryExecutionContext["Database"] == DB and WorkGroup == SETTINGS.athena_workgroup
        self.queries.append(QueryString)
        return {"QueryExecutionId": str(len(self.queries) - 1)}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"},
                                   "Statistics": {"DataScannedInBytes": 1024, "TotalExecutionTimeInMillis": 5}}}

    def get_paginator(self, name):
        fake = self

        class P:
            def paginate(self, QueryExecutionId):
                sql = fake.queries[int(QueryExecutionId)]
                v = next((n for m, n in fake.violations.items() if m in sql), 0)
                yield {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "violations"}]},
                                              {"Data": [{"VarCharValue": str(v)}]}]}}
        return P()


def test_all_validation_files_return_violations_column():
    files = sorted(VALIDATIONS_DIR.glob("v*.sql"))
    assert len(files) == 10
    for f in files:
        assert "AS violations" in f.read_text(encoding="utf-8"), f.name


def test_validations_pass_and_run_id_is_injected(aws):
    s3, _ = aws
    fake = FakeAthena({})
    out = run_validations(SETTINGS, RUN1, athena=fake, s3=s3)
    assert out["passed"] and len(out["checks"]) == 10
    assert any(f"'{RUN1}'" in q for q in fake.queries)
    assert s3_utils.object_exists(s3, BUCKET, f"control/runs/run_id={RUN1}/validations.json")


def test_failed_validation_fails_the_task(aws):
    s3, _ = aws
    with pytest.raises(ValidationFailed, match="v01_unicidad_news_id"):
        run_validations(SETTINGS, RUN1, athena=FakeAthena({"count(DISTINCT news_id) AS violations": 2}), s3=s3)
    summary = s3_utils.read_json(s3, BUCKET, f"control/runs/run_id={RUN1}/validations.json")
    assert summary["failed_checks"] == ["v01_unicidad_news_id"]
