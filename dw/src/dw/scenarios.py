"""Ejecuta y documenta los 6 escenarios de demostración del Data Warehouse.

Se corre DENTRO del contenedor del scheduler de Airflow (tiene el CLI de Airflow, el paquete dw y el
rol IAM de la EC2):

    docker compose exec airflow-scheduler python -m dw.scenarios            # los 6, en orden
    docker compose exec airflow-scheduler python -m dw.scenarios --only 4   # uno solo

Cada escenario prepara las fuentes, dispara el DAG dw_ecommerce_pipeline con los parámetros que
corresponden, espera el resultado y consulta S3 y Redshift para dejar la evidencia en:
  * s3://<bucket>/control/scenarios/<n>_<nombre>.json
  * dw/evidencias/scenarios/<n>_<nombre>.json   (carpeta montada: queda en la EC2 para el repo)

  1 carga_inicial      generate_data (con defectuosos) + extraction_mode=full
  2 defectuosos        cuarentena por motivo y lote de origen; pruebas dbt en verde
  3 repeticion         reprocesa el lote 1: mismas filas en raw_data y en fct_sales
  4 incremental        append_batch --batch-id 1 + modo auto: solo llegan los registros nuevos
  5 scd2               simulate_changes: nueva versión de cliente y producto, hechos point-in-time
  6 fallo_controlado   append_batch 2 + simulate_failure: ROLLBACK, marca sin avanzar; reproceso OK
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dw import batches, s3io
from dw.connections import redshift_query
from dw.entities import ALL_ENTITIES
from dw.settings import load_settings

DAG_ID = "dw_ecommerce_pipeline"
LOCAL_DIR = Path("/opt/airflow/dw/evidencias/scenarios")
log = logging.getLogger("dw.scenarios")


# ----------------------------------------------------------------------------- utilidades
def sh(*args: str, check: bool = True) -> str:
    log.info("$ %s", " ".join(args))
    proc = subprocess.run(args, capture_output=True, text=True)
    out = (proc.stdout + proc.stderr).strip()
    if out:
        log.info(out[-3000:])
    if check and proc.returncode != 0:
        raise RuntimeError(f"falló: {' '.join(args)}")
    return proc.stdout


def source_script(module: str, *args: str) -> str:
    return sh(sys.executable, "-m", f"dw.sources.{module}", *args)


def trigger(label: str, conf: dict, timeout_s: int = 1800) -> dict:
    """Dispara el DAG y espera a que termine. Devuelve run_id, estado y duración."""
    run_id = f"scenario_{label}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    sh("airflow", "dags", "unpause", DAG_ID)
    sh("airflow", "dags", "trigger", DAG_ID, "--run-id", run_id, "--conf", json.dumps(conf))
    start = time.time()
    state = "queued"
    while time.time() - start < timeout_s:
        time.sleep(20)
        runs = json.loads(sh("airflow", "dags", "list-runs", "-d", DAG_ID, "-o", "json") or "[]")
        state = next((r["state"] for r in runs if r["run_id"] == run_id), state)
        log.info("[scenario=%s] run %s: %s (%ss)", label, run_id, state, int(time.time() - start))
        if state in ("success", "failed"):
            break
    return {"airflow_run_id": run_id, "conf": conf, "state": state,
            "elapsed_s": int(time.time() - start)}


def batch_of(settings, s3, airflow_run_id: str) -> str | None:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", airflow_run_id)   # igual que batches.resolve_batch_id
    key = f"control/airflow_runs/{safe}.json"
    return s3io.read_json(s3, settings.bucket, key)["batch_id"] if s3io.exists(s3, settings.bucket, key) else None


def control(settings, s3, batch_id: str, name: str):
    key = batches.control_key(batch_id, name)
    return s3io.read_json(s3, settings.bucket, key) if s3io.exists(s3, settings.bucket, key) else None


def q(settings, sql: str) -> list[list]:
    return [[str(v) if v is not None else None for v in row] for row in redshift_query(settings, sql)]


def raw_rows_for_batch(settings, batch_id: str) -> dict:
    parts = " union all ".join(
        f"select '{e.raw_table}' as t, count(*) from {e.raw_table} where _batch_id = '{batch_id}'"
        for e in ALL_ENTITIES)
    return {t: int(n) for t, n in q(settings, parts)}


def fact_totals(settings) -> dict:
    """Filas de los hechos y de los snapshots SCD2 (repetir un lote no debe crear versiones)."""
    out = {}
    for snap in ("snap_dim_customer", "snap_dim_product"):
        out[snap] = {"versions": int(q(settings, f"select count(*) from snapshots.{snap}")[0][0])}
    for table, measure in (("fct_sales", "sum(subtotal)"), ("fct_reviews", "avg(rating)"),
                           ("fct_navigation_events", "null")):
        n, m = q(settings, f"select count(*), {measure} from marts.{table}")[0]
        out[table] = {"rows": int(n), "measure": m}
    return out


def manifest_summary(manifest: dict | None) -> dict | None:
    if not manifest:
        return None
    return {"mode": manifest["mode"], "watermarks_from_batch": manifest["watermarks_from_batch"],
            "extracted_at_utc": manifest["extracted_at_utc"],
            "entities": {e["entity"]: {"rows": e["rows"], "from": e["watermark_from"], "to": e["watermark_to"]}
                         for e in manifest["entities"]}}


def batch_evidence(settings, s3, batch_id: str) -> dict:
    return {
        "batch_id": batch_id,
        "status": control(settings, s3, batch_id, "status"),
        "extract_mysql": manifest_summary(control(settings, s3, batch_id, "extract_mysql")),
        "extract_documentdb": manifest_summary(control(settings, s3, batch_id, "extract_documentdb")),
        "load": control(settings, s3, batch_id, "load"),
        "reconciliation": control(settings, s3, batch_id, "reconciliation"),
        "dbt_test": _test_summary(control(settings, s3, batch_id, "dbt_test")),
        "raw_rows_with_this_batch_id": raw_rows_for_batch(settings, batch_id),
    }


def _test_summary(summary: dict | None) -> dict | None:
    if not summary:
        return None
    statuses: dict[str, int] = {}
    for r in summary["results"]:
        statuses[r["status"]] = statuses.get(r["status"], 0) + 1
    return {"total": len(summary["results"]), "by_status": statuses}


def save(settings, s3, number: int, name: str, payload: dict) -> dict:
    payload = {"scenario": number, "name": name,
               "recorded_at_utc": datetime.now(timezone.utc).isoformat(), **payload}
    s3io.put_json(s3, settings.bucket, f"control/scenarios/{number}_{name}.json", payload)
    try:
        LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        (LOCAL_DIR / f"{number}_{name}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    except OSError as exc:
        log.warning("no se pudo escribir la copia local: %s", exc)
    verdict = "OK" if payload.get("passed") else "REVISAR"
    log.info("==== Escenario %s (%s): %s", number, name, verdict)
    return payload


def _first_batch(settings, s3) -> str:
    path = LOCAL_DIR / "1_carga_inicial.json"
    if path.exists():
        return json.loads(path.read_text())["batch_id"]
    key = "control/scenarios/1_carga_inicial.json"
    return s3io.read_json(s3, settings.bucket, key)["batch_id"]


# ----------------------------------------------------------------------------- escenarios
def scenario_1(settings, s3):
    source_script("generate_data")          # 50 clientes, 25 productos, 100 pedidos, defectuosos
    run = trigger("1_carga_inicial", {"extraction_mode": "full", "reprocess_batch_id": None,
                                      "simulate_failure": False})
    bid = batch_of(settings, s3, run["airflow_run_id"])
    ev = batch_evidence(settings, s3, bid)
    return save(settings, s3, 1, "carga_inicial", {
        "batch_id": bid, "run": run, **ev, "facts": fact_totals(settings),
        "passed": run["state"] == "success" and ev["extract_mysql"]["mode"] == "full"
                  and bool(ev["reconciliation"] and ev["reconciliation"]["passed"]),
    })


QUARANTINE = ["mysql__customers", "mysql__products", "mysql__orders", "mysql__order_items",
              "mongodb__reviews", "mongodb__navigation_events"]


def scenario_2(settings, s3):
    sql = " union all ".join(
        f"select '{t}', rejection_reason, _batch_id, count(*) from quarantine.quarantine_{t} "
        f"group by rejection_reason, _batch_id" for t in QUARANTINE)
    rows = q(settings, sql + " order by 1, 2")
    bid = _first_batch(settings, s3)
    tests = _test_summary(control(settings, s3, bid, "dbt_test"))
    return save(settings, s3, 2, "defectuosos", {
        "batch_id": bid,
        "quarantine": [{"entity": e, "rejection_reason": r, "source_batch_id": b, "rows": int(n)}
                       for e, r, b, n in rows],
        "dbt_test": tests,
        "explanation": "Los registros inválidos no llegan a marts: quedan en quarantine.* con su motivo "
                       "y el _batch_id de origen. Las pruebas de dbt pasan sobre los datos válidos.",
        "passed": len(rows) > 0 and bool(tests) and set(tests["by_status"]) <= {"pass"},
    })


def scenario_3(settings, s3):
    bid = _first_batch(settings, s3)
    before = {"raw": raw_rows_for_batch(settings, bid), "facts": fact_totals(settings)}
    extracted_before = control(settings, s3, bid, "extract_mysql")["extracted_at_utc"]
    run = trigger("3_repeticion", {"reprocess_batch_id": bid, "extraction_mode": "auto",
                                   "simulate_failure": False})
    after = {"raw": raw_rows_for_batch(settings, bid), "facts": fact_totals(settings)}
    extracted_after = control(settings, s3, bid, "extract_mysql")["extracted_at_utc"]
    return save(settings, s3, 3, "repeticion", {
        "batch_id": bid, "run": run, "before": before, "after": after,
        "source_not_queried_again": extracted_before == extracted_after,
        "explanation": "El reproceso reutiliza los Parquet del lote (no consulta las fuentes) y la carga "
                       "hace DELETE del lote + COPY en una transacción: raw_data y los hechos no cambian.",
        "passed": run["state"] == "success" and before == after and extracted_before == extracted_after,
    })


def scenario_4(settings, s3):
    before = fact_totals(settings)
    source_script("append_batch", "--batch-id", "1")
    run = trigger("4_incremental", {"extraction_mode": "auto", "reprocess_batch_id": None,
                                    "simulate_failure": False})
    bid = batch_of(settings, s3, run["airflow_run_id"])
    ev = batch_evidence(settings, s3, bid)
    after = fact_totals(settings)
    return save(settings, s3, 4, "incremental", {
        "batch_id": bid, "run": run, **ev, "facts_before": before, "facts_after": after,
        "new_sales_lines": after["fct_sales"]["rows"] - before["fct_sales"]["rows"],
        "explanation": "Modo incremental: cada entidad parte de la marca (watermark_to) del último lote "
                       "COMPLETE; pedidos, líneas, reseñas y eventos por id > marca, clientes y productos "
                       "por updated_at >= marca.",
        "passed": run["state"] == "success" and ev["extract_mysql"]["mode"] == "incremental"
                  and ev["extract_mysql"]["entities"]["orders"]["rows"] == 3
                  and after["fct_sales"]["rows"] - before["fct_sales"]["rows"] == 3,
    })


def scenario_5(settings, s3):
    source_script("simulate_changes", "--customer-id", "1", "--city", "Medellín", "--department", "Antioquia",
                  "--product-id", "1", "--category", "Hogar", "--changed-at", "2025-07-01")
    run = trigger("5_scd2", {"extraction_mode": "auto", "reprocess_batch_id": None, "simulate_failure": False})
    bid = batch_of(settings, s3, run["airflow_run_id"])
    customer = q(settings, "select customer_sk, city, department, valid_from, valid_to, is_current "
                           "from marts.dim_customer where customer_id = 1 order by valid_from")
    product = q(settings, "select product_sk, category, valid_from, valid_to, is_current "
                          "from marts.dim_product where product_id = 1 order by valid_from")
    point_in_time = q(settings, """
        select c.city, case when d.date < '2025-07-01' then 'antes' else 'después' end as periodo,
               count(*) as lineas
        from marts.fct_sales f
        join marts.dim_customer c on c.customer_sk = f.customer_sk
        join marts.dim_date d on d.date_sk = f.date_sk
        where c.customer_id = 1
        group by 1, 2 order by 2, 1""")
    return save(settings, s3, 5, "scd2", {
        "batch_id": bid, "run": run,
        "dim_customer_id_1": [dict(zip(["customer_sk", "city", "department", "valid_from", "valid_to", "is_current"], r))
                              for r in customer],
        "dim_product_id_1": [dict(zip(["product_sk", "category", "valid_from", "valid_to", "is_current"], r))
                             for r in product],
        "fct_sales_customer_1_by_period": [dict(zip(["city", "period", "lines"], r)) for r in point_in_time],
        "explanation": "dbt snapshot (estrategia timestamp sobre updated_at) cierra la versión anterior y abre "
                       "una nueva desde 2025-07-01; las ventas anteriores conservan la ciudad vieja.",
        "passed": run["state"] == "success" and len(customer) == 2 and len(product) == 2,
    })


def scenario_6(settings, s3):
    last_complete_before = batches.last_complete_batch(s3, settings.bucket)
    facts_before = fact_totals(settings)
    source_script("append_batch", "--batch-id", "2")
    failed_run = trigger("6_fallo", {"extraction_mode": "auto", "reprocess_batch_id": None,
                                     "simulate_failure": True})
    bid = batch_of(settings, s3, failed_run["airflow_run_id"])
    after_failure = {
        "status": control(settings, s3, bid, "status"),
        "raw_rows_with_this_batch_id": raw_rows_for_batch(settings, bid),
        "last_complete_batch": batches.last_complete_batch(s3, settings.bucket),
        "facts": fact_totals(settings),
    }
    retry_run = trigger("6_reproceso", {"reprocess_batch_id": bid, "extraction_mode": "auto",
                                        "simulate_failure": False})
    ev = batch_evidence(settings, s3, bid)
    facts_after = fact_totals(settings)
    return save(settings, s3, 6, "fallo_controlado", {
        "batch_id": bid, "failed_run": failed_run, "after_failure": after_failure,
        "last_complete_before": last_complete_before, "retry_run": retry_run, "after_reprocess": ev,
        "facts_before": facts_before, "facts_after": facts_after,
        "explanation": "La carga falla después del COPY: ROLLBACK (0 filas del lote en raw_data), el lote "
                       "queda FAILED tras 2 reintentos de 30 s y la marca de avance sigue en el último lote "
                       "COMPLETE. El reproceso del MISMO batch_id termina COMPLETE sin duplicar.",
        "passed": failed_run["state"] == "failed"
                  and (after_failure["status"] or {}).get("status") == batches.FAILED
                  and sum(after_failure["raw_rows_with_this_batch_id"].values()) == 0
                  and after_failure["last_complete_batch"] == last_complete_before
                  and retry_run["state"] == "success"
                  and facts_after["fct_sales"]["rows"] - facts_before["fct_sales"]["rows"] == 3,
    })


SCENARIOS = {1: scenario_1, 2: scenario_2, 3: scenario_3, 4: scenario_4, 5: scenario_5, 6: scenario_6}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", type=int, choices=sorted(SCENARIOS), action="append",
                        help="Ejecuta solo ese escenario (se puede repetir)")
    args = parser.parse_args()
    settings = load_settings()
    s3 = s3io.client(settings.region)
    results = {}
    for n in args.only or sorted(SCENARIOS):
        results[n] = SCENARIOS[n](settings, s3)["passed"]
    log.info("Resumen: %s", {n: ("OK" if ok else "REVISAR") for n, ok in results.items()})
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
