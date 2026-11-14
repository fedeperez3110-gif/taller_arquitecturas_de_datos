"""Ejecuta consultas en Athena y las validaciones de calidad del lote (requisito 4.5).

Cada archivo de src/datalake/sql_validations/ devuelve una sola columna `violations`;
un valor distinto de 0 indica un problema y hace fallar la tarea (el lote queda en FAILED).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from datalake import s3_utils
from datalake.config import Settings, load_settings

log = logging.getLogger(__name__)

VALIDATIONS_DIR = Path(__file__).parent / "sql_validations"
POLL_SECONDS = 1.0
TIMEOUT_SECONDS = 180


class ValidationFailed(RuntimeError):
    pass


def run_query(athena, settings: Settings, sql: str) -> tuple[list[dict], dict]:
    """Ejecuta una consulta y espera el resultado. Devuelve (filas, estadísticas)."""
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": settings.glue_database},
        WorkGroup=settings.athena_workgroup,
    )["QueryExecutionId"]
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        ex = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Consulta Athena {qid} superó {TIMEOUT_SECONDS}s")
        time.sleep(POLL_SECONDS)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Consulta Athena {qid} {state}: {ex['Status'].get('StateChangeReason')}")

    rows: list[dict] = []
    header: list[str] | None = None
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue") for c in r["Data"]]
            if header is None:
                header = values
            else:
                rows.append(dict(zip(header, values)))
    stats = {"query_id": qid, "bytes_scanned": ex["Statistics"].get("DataScannedInBytes", 0),
             "elapsed_ms": ex["Statistics"].get("TotalExecutionTimeInMillis", 0)}
    return rows, stats


def run_validations(settings: Settings, run_id: str, athena=None, s3=None) -> dict:
    athena = athena or boto3.client("athena", region_name=settings.region)
    s3 = s3 or s3_utils.s3_client(settings.region)
    results = []
    for path in sorted(VALIDATIONS_DIR.glob("v*.sql")):
        sql = path.read_text(encoding="utf-8").replace("{run_id}", run_id)
        rows, stats = run_query(athena, settings, sql)
        violations = int(rows[0]["violations"] or 0) if rows else 0
        results.append({"check": path.stem, "violations": violations, **stats})
        level = logging.INFO if violations == 0 else logging.ERROR
        log.log(level, "[run_id=%s][task=validate_with_athena] %s violaciones=%s bytes=%s",
                run_id, path.stem, violations, stats["bytes_scanned"])

    failed = [r["check"] for r in results if r["violations"]]
    summary = {"run_id": run_id, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
               "passed": not failed, "failed_checks": failed, "checks": results}
    s3_utils.put_json(s3, settings.bucket, f"control/runs/run_id={run_id}/validations.json", summary)
    if failed:
        raise ValidationFailed(f"[run_id={run_id}] validaciones fallidas: {failed}")
    return summary


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_validations(load_settings(), sys.argv[1])
