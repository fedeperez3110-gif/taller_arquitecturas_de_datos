"""Pruebas del recolector de evidencias (sin Airflow ni AWS reales)."""
import boto3
import pytest
from moto import mock_aws

from datalake.evidence import Report, md_table, section_scenarios, split_statements
from tests.test_silver import BUCKET


def test_split_statements_keeps_titles():
    sql = "-- 06a. Sin filtro\nSELECT 1;\n\n-- 06b. Con filtro\n-- detalle\nSELECT 2\nFROM t;\n"
    assert split_statements(sql) == [("06a. Sin filtro", "SELECT 1"), ("06b. Con filtro", "SELECT 2\nFROM t")]


def test_md_table_escapes_and_limits():
    out = md_table([{"a": "x|y", "b": [1, 2]}] * 3, limit=2)
    assert "x\\|y" in out and "1, 2" in out and "1 filas más" in out


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def run(airflow_id, conf=None, retried=False):
    tasks = [{"task_id": "build_gold", "try_number": 2 if retried else 1, "state": "success"}]
    return {"airflow_run_id": airflow_id, "state": "success", "conf": conf or {}, "tasks": tasks}


def stage(new=0, updated=0, unchanged=0, rejected=0, analyzed=0, controlled=None, added=None, passed=True):
    st = {"silver": {"totals": {"new": new, "updated": updated, "unchanged": unchanged, "rejected": rejected}},
          "comprehend": {"texts_analyzed": analyzed, "texts_from_cache": 0},
          "bronze": {"controlled_copy_of": controlled} if controlled else {},
          "validations": {"passed": passed}}
    if added is not None:
        st["catalog"] = {"tables": [{"table": "silver_news", "added": added}]}
    return st


def test_all_scenarios_are_detected(s3):
    runs = [run("a"), run("b", {"reprocess_run_id": "R1"}), run("c"),
            run("d", {"reprocess_run_id": "R4"}), run("e", {"simulate_failure": "build_gold"}, retried=True)]
    ctrl = {
        "R1": {**stage(new=30, analyzed=30, added=["2026-11-10", "2026-11-11"])},
        "R2": {**stage(unchanged=30, added=[])},
        "R3": {**stage(new=30, unchanged=30, analyzed=30, added=["2026-11-13"])},
        "R4": {**stage(new=1, updated=2, rejected=2, analyzed=3, controlled="R3", added=["unknown"])},
        "R5": {**stage(unchanged=60, added=[])},
    }
    for rid, a in zip(ctrl, "abcde"):
        ctrl[rid]["final"] = {"airflow_run_id": a}

    status = section_scenarios(Report(s3, BUCKET), s3, BUCKET, ctrl, runs, [])
    assert all(status.values()), status
