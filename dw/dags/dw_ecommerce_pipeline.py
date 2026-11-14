"""Pipeline del Data Warehouse de e-commerce en AWS.

validación de parámetros y conexiones -> RDS MySQL + DocumentDB --(extracción full/incremental)--> S3 raw/ --(COPY)--> Redshift raw_data
--(dbt: staging, calidad, cuarentena, snapshots SCD2, marts, pruebas)--> conciliación --> estado.

Cada ejecución tiene un batch_id (hora real UTC de inicio, AAAAMMDDTHHMMSSZ) que aparece en S3,
en las columnas _batch_id de Redshift y en cada línea de log como [batch_id=...][task=...].

Parámetros al ejecutar con "Trigger DAG w/ config":
  * reprocess_batch_id: vacío = lote nuevo; un batch_id existente = reprocesarlo (reutiliza los
    archivos de S3 sin volver a consultar las fuentes; no duplica en Redshift).
  * extraction_mode: auto (incremental si hay un lote COMPLETE previo) | full.
  * simulate_failure: true = falla la carga después del COPY (ROLLBACK y marca sin avanzar).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("dw_pipeline")


def _settings():
    from dw.settings import load_settings
    return load_settings()


def _batch_id() -> str:
    ctx = get_current_context()
    return ctx["ti"].xcom_pull(task_ids="assign_batch")


def _ctx(batch_id: str) -> str:
    return f"[batch_id={batch_id}][task={get_current_context()['ti'].task_id}]"


@dag(
    dag_id="dw_ecommerce_pipeline",
    description="MySQL + DocumentDB -> S3 -> Redshift -> dbt (Data Warehouse de e-commerce)",
    start_date=datetime(2026, 1, 1),
    schedule=None,              # se ejecuta a demanda (y desde dw.scenarios)
    catchup=False,
    max_active_runs=1,          # dos lotes simultáneos competirían por la marca de avance
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "owner": "dw"},
    params={
        "reprocess_batch_id": Param(None, type=["null", "string"],
                                    description="Vacío = lote nuevo. AAAAMMDDTHHMMSSZ = reprocesar ese lote."),
        "extraction_mode": Param("auto", enum=["auto", "full"],
                                 description="auto = incremental desde el último lote COMPLETE; full = todo."),
        "simulate_failure": Param(False, type="boolean",
                                  description="Falla la carga a Redshift después del COPY (prueba de ROLLBACK)."),
    },
    render_template_as_native_obj=True,
    tags=["data-warehouse", "redshift", "dbt"],
)
def dw_ecommerce_pipeline():

    @task(retries=0)
    def assign_batch() -> str:
        from dw import batches, s3io
        ctx = get_current_context()
        settings = _settings()
        s3 = s3io.client(settings.region)
        reprocess = (ctx["params"].get("reprocess_batch_id") or "").strip()
        if reprocess:
            batches.validate_batch_id(reprocess)
            if reprocess not in batches.list_batches(s3, settings.bucket):
                raise ValueError(f"No existe el lote {reprocess} en s3://{settings.bucket}/control/batches/")
            batch_id = reprocess
            log.info("[batch_id=%s][task=assign_batch] REPROCESO de un lote existente", batch_id)
        else:
            batch_id = batches.resolve_batch_id(s3, settings.bucket, ctx["run_id"])
            log.info("[batch_id=%s][task=assign_batch] lote nuevo para el run %s", batch_id, ctx["run_id"])
        return batch_id

    @task(retries=1)
    def validate_connections() -> dict:
        from dw.validate import validate
        return validate(_settings(), _batch_id(), dict(get_current_context()["params"]))

    def _extract(source: str) -> dict:
        from dw.extract import extract_source
        batch_id = _batch_id()
        mode = get_current_context()["params"].get("extraction_mode") or "auto"
        manifest = extract_source(_settings(), source, batch_id, mode=mode)
        log.info("%s %s modo=%s filas=%s", _ctx(batch_id), manifest["action"], manifest["mode"],
                 manifest["total_rows"])
        return {"action": manifest["action"], "mode": manifest["mode"], "rows": manifest["total_rows"]}

    @task
    def extract_mysql() -> dict:
        return _extract("mysql")

    @task
    def extract_documentdb() -> dict:
        return _extract("documentdb")

    @task
    def load_redshift() -> dict:
        from dw.load import load_batch
        batch_id = _batch_id()
        fail = bool(get_current_context()["params"].get("simulate_failure"))
        if fail:
            log.warning("%s simulate_failure=true: la carga va a fallar después del COPY", _ctx(batch_id))
        summary = load_batch(_settings(), batch_id, simulate_failure=fail)
        log.info("%s COPY de %s filas en %s tablas", _ctx(batch_id), summary["total_rows"], len(summary["entities"]))
        return {"total_rows": summary["total_rows"]}

    def _dbt(command: str, selector: str | None = None, label: str | None = None):
        from dw.dbt_runner import run_dbt
        batch_id = _batch_id()
        summary = run_dbt(_settings(), batch_id, command, selector=selector, label=label)
        if summary:
            bad = [r for r in summary["results"] if r["status"] not in ("success", "pass")]
            log.info("%s %s nodos, %s con problemas", _ctx(batch_id), len(summary["results"]), len(bad))

    @task
    def dbt_deps():
        from pathlib import Path
        settings = _settings()
        if (Path(settings.dbt_project_dir) / "dbt_packages" / "dbt_utils").exists():
            log.info("%s dbt_utils ya instalado", _ctx(_batch_id()))
            return
        _dbt("deps")

    @task
    def dbt_seed():
        _dbt("seed")

    @task
    def dbt_run_intermediate():
        # staging (versión vigente por llave), reglas de calidad y cuarentena con su lote de origen
        _dbt("run", "intermediate quarantine", label="run_intermediate")

    @task
    def dbt_snapshot():
        # SCD2 de clientes y productos
        _dbt("snapshot")

    @task
    def dbt_run_marts():
        _dbt("run", "marts", label="run_marts")

    @task
    def dbt_test():
        _dbt("test")

    @task
    def reconcile() -> dict:
        from dw.reconcile import reconcile as run
        result = run(_settings(), _batch_id())
        return {"passed": result["passed"], "checks": result["checks"]}

    @task(retries=0)
    def mark_complete():
        from dw import batches, s3io
        settings, batch_id = _settings(), _batch_id()
        ctx = get_current_context()
        batches.record_status(s3io.client(settings.region), settings.bucket, batch_id, batches.COMPLETE, {
            "airflow_run_id": ctx["run_id"], "params": dict(ctx["params"]),
        })
        log.info("%s lote COMPLETE: la marca de avance queda en este lote", _ctx(batch_id))

    @task(retries=0, trigger_rule=TriggerRule.ONE_FAILED)
    def mark_failed():
        from dw import batches, s3io
        ctx = get_current_context()
        batch_id = ctx["ti"].xcom_pull(task_ids="assign_batch")
        if not batch_id:
            log.error("[batch_id=-][task=mark_failed] falló antes de asignar el lote")
            return
        settings = _settings()
        failed = [ti.task_id for ti in ctx["dag_run"].get_task_instances(state="failed")]
        batches.record_status(s3io.client(settings.region), settings.bucket, batch_id, batches.FAILED, {
            "airflow_run_id": ctx["run_id"], "failed_tasks": failed, "params": dict(ctx["params"]),
        })
        log.error("%s lote FAILED en %s: la marca de avance NO se mueve; reprocesa con "
                  "reprocess_batch_id=%s", _ctx(batch_id), failed, batch_id)

    batch = assign_batch()
    check = validate_connections()
    extracts = [extract_mysql(), extract_documentdb()]
    load = load_redshift()
    deps, seed, inter, snap, marts, tests = (dbt_deps(), dbt_seed(), dbt_run_intermediate(),
                                             dbt_snapshot(), dbt_run_marts(), dbt_test())
    rec, done, failed = reconcile(), mark_complete(), mark_failed()

    batch >> check >> extracts >> load >> deps >> seed >> inter >> snap >> marts >> tests >> rec >> done
    [batch, check, *extracts, load, deps, seed, inter, snap, marts, tests, rec] >> failed


dw_ecommerce_pipeline()
