"""DAG del data lake de noticias RSS.

Flujo (sin intervención manual entre etapas):
  validate_config -> ingest_bronze -> build_silver -> enrich_comprehend -> build_gold
  -> register_partitions -> validate_with_athena -> record_run_status

Parámetros al disparar manualmente ("Trigger DAG"):
  reprocess_run_id  : "20261022T113000Z" -> NO consulta los RSS; reconstruye todo desde ese lote de Bronze.
  simulate_failure  : nombre de una tarea -> esa tarea falla A PROPÓSITO en su primer intento, DESPUÉS
                      de escribir sus resultados. Airflow la reintenta y el reintento debe dejar los mismos
                      resultados (sin pérdida ni duplicados). Escenario "Fallo controlado y reejecución".
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

log = logging.getLogger("rss_datalake")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,                                # reintentos acotados
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}


def _run_id_from_context() -> str:
    """run_id del lote: reprocess_run_id si se pidió; si no, la HORA REAL de inicio de la ejecución.

    No se usa la fecha lógica de Airflow porque, si la ejecución corre tarde (p. ej. la EC2 estuvo
    apagada), el lote quedaría con una fecha de ingesta falsa. La hora real se guarda en S3 la
    primera vez y los reintentos de la misma ejecución la reutilizan (idempotencia).
    """
    from datalake import s3_utils
    from datalake.config import load_settings
    from datalake.ingest_bronze import resolve_batch_run_id, validate_run_id

    ctx = get_current_context()
    reprocess = (ctx["params"].get("reprocess_run_id") or "").strip()
    if reprocess:
        return validate_run_id(reprocess)
    settings = load_settings()
    return resolve_batch_run_id(s3_utils.s3_client(settings.region), settings.bucket,
                                ctx["dag_run"].run_id)


def _maybe_fail(task_name: str, run_id: str) -> None:
    """Fallo controlado: se lanza al FINAL de la tarea (con los archivos ya escritos) y solo en el
    primer intento, para demostrar que el reintento recupera sin duplicar resultados."""
    ctx = get_current_context()
    if ctx["params"].get("simulate_failure") == task_name and ctx["ti"].try_number == 1:
        log.error("[run_id=%s][task=%s] FALLO CONTROLADO (simulate_failure). Airflow reintentará.",
                  run_id, task_name)
        raise AirflowException(f"Fallo controlado en {task_name}")


@dag(
    dag_id="rss_datalake",
    description="RSS El Tiempo -> S3 (Bronze/Silver/Gold) -> Comprehend -> Glue -> Athena",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule="0 */2 * * *",       # cada 2 horas mientras el DAG esté activo (laboratorio)
    catchup=False,
    max_active_runs=1,            # control de concurrencia: nunca dos lotes escribiendo a la vez
    default_args=DEFAULT_ARGS,
    params={
        "reprocess_run_id": Param(
            None, type=["null", "string"],
            description="Si se indica (AAAAMMDDTHHMMSSZ), reprocesa ese lote desde Bronze sin consultar los RSS.",
        ),
        "simulate_failure": Param(
            None, type=["null", "string"],
            description="Fallo controlado (demostración). Vacío = normal. Valores: build_silver, "
                        "enrich_comprehend, build_gold, register_partitions.",
        ),
    },
    tags=["datalake", "rss"],
)
def rss_datalake():

    @task
    def validate_config() -> dict:
        """1. Valida configuración y acceso a S3 antes de tocar las fuentes."""
        from datalake.config import load_settings
        from datalake import s3_utils

        settings = load_settings()
        run_id = _run_id_from_context()
        s3 = s3_utils.s3_client(settings.region)
        s3.head_bucket(Bucket=settings.bucket)   # falla rápido si no hay permisos o el bucket no existe
        log.info("[run_id=%s][task=validate_config] bucket=%s fuentes=%s", run_id,
                 settings.bucket, [s.source_id for s in settings.sources])
        return {"run_id": run_id, "bucket": settings.bucket}

    @task
    def ingest_bronze(cfg: dict) -> dict:
        """2. Extrae los feeds y escribe el contenido original en Bronze."""
        from datalake import s3_utils
        from datalake.config import load_settings
        from datalake.ingest_bronze import RUN_FAILED, RUN_PARTIAL, ingest_all, run_control_key

        ctx = get_current_context()
        settings = load_settings()
        run_id = cfg["run_id"]

        if ctx["params"].get("reprocess_run_id"):
            s3 = s3_utils.s3_client(settings.region)
            key = run_control_key(run_id, "bronze")
            if not s3_utils.object_exists(s3, settings.bucket, key):
                raise AirflowException(f"[run_id={run_id}] no existe en Bronze; no se puede reprocesar")
            summary = s3_utils.read_json(s3, settings.bucket, key)
            log.info("[run_id=%s][task=ingest_bronze] REPROCESO: se reutiliza Bronze, sin consultar RSS", run_id)
            return {"run_id": run_id, "run_status": summary["run_status"], "mode": "reprocess"}

        summary = ingest_all(settings, run_id)
        status = summary["run_status"]
        ti = ctx["ti"]
        retries_left = ti.try_number <= ti.max_tries

        if status == RUN_FAILED:
            raise AirflowException(f"[run_id={run_id}] ninguna fuente respondió correctamente")
        if status == RUN_PARTIAL and retries_left:
            # Se reintenta: las fuentes exitosas se saltan (idempotencia), solo se reintentan las fallidas.
            raise AirflowException(f"[run_id={run_id}] lote parcial; se reintentarán las fuentes fallidas")
        if status == RUN_PARTIAL:
            log.warning("[run_id=%s][task=ingest_bronze] lote PARCIAL tras agotar reintentos; se continúa "
                        "solo con las fuentes válidas", run_id)
        return {"run_id": run_id, "run_status": status, "mode": "new_extraction",
                "items_received": summary["items_received"]}

    @task
    def build_silver(bronze: dict) -> dict:
        """3. Limpieza, validación, deduplicación y publicación de Silver (sin consultar los RSS)."""
        from datalake.config import load_settings
        from datalake.silver import build_silver as run_silver

        summary = run_silver(load_settings(), bronze["run_id"])
        _maybe_fail("build_silver", bronze["run_id"])
        return {"run_id": bronze["run_id"], "bronze_status": bronze["run_status"],
                "silver_totals": summary["totals"], "news_current_total": summary["news_current_total"]}

    @task
    def enrich_comprehend(silver: dict) -> dict:
        """4. Entidades y frases clave con Amazon Comprehend (solo textos nuevos o modificados)."""
        from datalake.config import load_settings
        from datalake.enrich import enrich

        summary = enrich(load_settings(), silver["run_id"])
        _maybe_fail("enrich_comprehend", silver["run_id"])
        return {**silver, "comprehend": {k: summary[k] for k in (
            "texts_analyzed", "texts_from_cache", "texts_failed", "entity_rows", "key_phrase_rows")}}

    @task
    def build_gold(enriched: dict) -> dict:
        """5. Agregados de Gold recalculados desde Silver vigente."""
        from datalake.config import load_settings
        from datalake.gold import build_gold as run_gold

        summary = run_gold(load_settings(), enriched["run_id"])
        _maybe_fail("build_gold", enriched["run_id"])
        return {**enriched, "gold_rows": summary["rows"]}

    @task
    def register_partitions(gold: dict) -> dict:
        """6. Registra en Glue Data Catalog las particiones nuevas (y quita las vacías)."""
        from datalake.catalog import sync_all
        from datalake.config import load_settings

        results = sync_all(load_settings(), gold["run_id"])
        _maybe_fail("register_partitions", gold["run_id"])
        return {**gold, "partitions_added": {r["table"]: r["added"] for r in results if r["added"]}}

    @task(retries=1)
    def validate_with_athena(registered: dict) -> dict:
        """7. Validaciones de calidad y conciliación en Athena. Si alguna falla, el lote falla."""
        from datalake.athena import run_validations
        from datalake.config import load_settings

        summary = run_validations(load_settings(), registered["run_id"])
        return {**registered, "validations": {c["check"]: c["violations"] for c in summary["checks"]}}

    @task(trigger_rule="all_done", retries=0)
    def record_run_status(result: dict | None = None) -> None:
        """Último paso: deja el estado final del lote en control/runs/."""
        from datalake import s3_utils
        from datalake.config import load_settings
        from datalake.ingest_bronze import run_control_key

        ctx = get_current_context()
        settings = load_settings()
        run_id = _run_id_from_context()
        dag_run = ctx["dag_run"]
        task_states = {ti.task_id: ti.state for ti in dag_run.get_task_instances()
                       if ti.task_id != "record_run_status"}
        failed = [t for t, s in task_states.items() if s not in ("success", "skipped")]
        final = "FAILED" if failed else (result or {}).get("bronze_status", "UNKNOWN")

        payload = {
            "run_id": run_id,
            "airflow_run_id": dag_run.run_id,
            "mode": "reprocess" if ctx["params"].get("reprocess_run_id") else "new_extraction",
            "final_status": final,               # COMPLETE | PARTIAL | FAILED
            "silver_totals": (result or {}).get("silver_totals"),
            "comprehend": (result or {}).get("comprehend"),
            "gold_rows": (result or {}).get("gold_rows"),
            "partitions_added": (result or {}).get("partitions_added"),
            "validations": (result or {}).get("validations"),
            "failed_tasks": failed,
            "task_states": task_states,
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        s3 = s3_utils.s3_client(settings.region)
        s3_utils.put_json(s3, settings.bucket, run_control_key(run_id, "final"), payload)
        log.info("[run_id=%s][task=record_run_status] estado final=%s fallidas=%s", run_id, final, failed)
        if failed:
            raise AirflowException(f"[run_id={run_id}] tareas fallidas: {failed}")

    cfg = validate_config()
    bronze = ingest_bronze(cfg)
    silver = build_silver(bronze)
    enriched = enrich_comprehend(silver)
    gold = build_gold(enriched)
    registered = register_partitions(gold)
    validated = validate_with_athena(registered)
    record_run_status(validated)


rss_datalake()
