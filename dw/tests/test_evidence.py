from datetime import datetime, timezone

from conftest import FakeMySQL
from dw import batches, evidence, s3io
from dw.extract import extract_source
from test_extract import mysql_tables

BID = "20261113T120000Z"


def test_report_sections_render_from_control_files(s3, settings):
    extract_source(settings, "mysql", BID, s3=s3, connection_factory=lambda _: FakeMySQL(mysql_tables()),
                   now=datetime(2026, 11, 13, tzinfo=timezone.utc))
    batches.record_status(s3, settings.bucket, BID, batches.COMPLETE, {})
    s3io.put_json(s3, settings.bucket, batches.control_key(BID, "dbt_test"), {
        "elapsed_s": 3.2, "results": [{"node": "test.ecommerce_dw.unique_fct_sales_sale_sk", "status": "pass",
                                       "failures": 0, "message": ""}]})
    pair = {"rows": 2, "subtotal": "21.00"}
    s3io.put_json(s3, settings.bucket, batches.control_key(BID, "reconciliation"), {
        "source_mysql": pair, "raw_latest_redshift": pair, "valid_intermediate": pair, "fct_sales": pair,
        "quarantined_order_items": 0, "passed": True})
    s3io.put_json(s3, settings.bucket, "control/scenarios/1_carga_inicial.json",
                  {"scenario": 1, "name": "carga_inicial", "batch_id": BID, "passed": True, "explanation": "x"})

    rep = evidence.Report()
    ctrl = evidence.batch_controls(s3, settings.bucket)
    evidence.section_batches(rep, s3, settings.bucket, ctrl)
    evidence.section_dbt(rep, ctrl)
    evidence.section_reconciliation(rep, ctrl)
    status = evidence.section_scenarios(rep, evidence.scenarios(s3, settings.bucket))
    rep.flush(s3, settings.bucket)

    assert status["1. Carga inicial"] and not status["6. Fallo controlado y reejecución"]
    assert "unique_fct_sales_sale_sk" in rep.files["05_dbt_pruebas.md"]
    assert "| 20261113T120000Z | COMPLETE | mysql | full |" in rep.files["03_lotes_s3.md"]
    assert "2 / 21.00" in rep.files["07_conciliacion.md"]
    assert s3io.exists(s3, settings.bucket, "evidence/08_escenarios.md")
