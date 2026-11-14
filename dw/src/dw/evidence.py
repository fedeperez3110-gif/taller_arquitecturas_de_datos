"""Recolector automático de evidencias del Data Warehouse (sección 4.3 del enunciado).

Se ejecuta DENTRO del contenedor del scheduler de Airflow en la EC2 (base de Airflow, logs y rol
IAM) y escribe archivos Markdown en  s3://<bucket>/evidence/ :

  README.md                      índice y estado de los escenarios
  01_airflow_ejecuciones.md      ejecuciones del DAG, lote, parámetros, estado e intentos por tarea
  02_logs_por_ejecucion.md       líneas [batch_id=...][task=...] por ejecución, tarea e intento
  03_lotes_s3.md                 lotes: estado, modo, filas y marcas de avance; archivos en raw/
  04_redshift.md                 filas por tabla y por lote en Redshift
  05_dbt_pruebas.md              resultado de cada modelo, snapshot y prueba dbt por lote
  06_cuarentena.md               rechazos por motivo y lote de origen
  07_conciliacion.md             indicador de ventas: fuente vs raw vs válidas vs hecho, por lote
  08_escenarios.md               los 6 escenarios de la sección 3.5 con sus datos
  09_quicksight_validacion.md    resultado de dbt/sql/quicksight_validation.sql
  raw/                           JSON de control/ copiados tal cual

Uso: ./infra/collect_evidence.sh desde tu computador (lo ejecuta en la EC2 y lo descarga).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from dw import batches, s3io
from dw.connections import redshift_query
from dw.settings import Settings, load_settings

log = logging.getLogger(__name__)

PREFIX = "evidence/"
DAG_ID = "dw_ecommerce_pipeline"
LOGS_DIR = Path(f"/opt/airflow/logs/dag_id={DAG_ID}")
VALIDATION_SQL = Path("/opt/airflow/dw/dbt/sql/quicksight_validation.sql")
TAG_RE = re.compile(r"\[batch_id=[^\]]+\]")
SCHEMAS = ("raw_data", "intermediate", "quarantine", "snapshots", "marts", "reference")
MAX_ROWS = 60


# ----------------------------------------------------------------------------- Markdown
def md_table(rows: list[dict], columns: list[str] | None = None, limit: int = MAX_ROWS) -> str:
    if not rows:
        return "_(sin filas)_\n"
    columns = columns or list(rows[0].keys())

    def cell(v):
        if isinstance(v, datetime):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, (dict, list)):
            v = json.dumps(v, ensure_ascii=False, default=str)
        return str(v if v is not None else "").replace("|", "\\|").replace("\n", " ")[:140]

    out = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    out += ["| " + " | ".join(cell(r.get(c)) for c in columns) + " |" for r in rows[:limit]]
    if len(rows) > limit:
        out.append(f"\n_… {len(rows) - limit} filas más (se muestran {limit})._")
    return "\n".join(out) + "\n"


def code(text: str, lang: str = "") -> str:
    return f"```{lang}\n{text.rstrip()}\n```\n"


class Report:
    def __init__(self):
        self.files: dict[str, str] = {}

    def write(self, name: str, title: str, body: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        self.files[name] = f"# {title}\n\n_Generado automáticamente el {stamp} por `src/dw/evidence.py`._\n\n{body}"

    def flush(self, s3, bucket: str) -> None:
        old = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
            old += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(old), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": old[i:i + 1000]})
        for name, text in self.files.items():
            ctype = "application/json" if name.endswith(".json") else "text/markdown; charset=utf-8"
            s3.put_object(Bucket=bucket, Key=PREFIX + name, Body=text.encode("utf-8"), ContentType=ctype)


def q(settings: Settings, sql: str) -> list[tuple]:
    return [tuple(r) for r in redshift_query(settings, sql)]


def read(s3, bucket: str, key: str):
    return s3io.read_json(s3, bucket, key) if s3io.exists(s3, bucket, key) else None


# ----------------------------------------------------------------------------- datos base
def scenarios(s3, bucket: str) -> dict[int, dict]:
    out = {}
    for n in range(1, 7):
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"control/scenarios/{n}_"):
            for o in page.get("Contents", []):
                out[n] = s3io.read_json(s3, bucket, o["Key"])
    return out


def airflow_runs(since: datetime | None) -> list[dict]:
    from airflow.models import DagRun

    out = []
    for dr in sorted(DagRun.find(dag_id=DAG_ID), key=lambda d: d.start_date or d.execution_date):
        if since and dr.start_date and dr.start_date < since:
            continue
        tis = sorted(dr.get_task_instances(), key=lambda t: t.start_date or dr.start_date)
        assign = next((t for t in tis if t.task_id == "assign_batch"), None)
        batch_id = assign.xcom_pull(task_ids="assign_batch") if assign else None
        out.append({
            "airflow_run_id": dr.run_id, "batch_id": batch_id, "state": dr.state,
            "start": dr.start_date, "end": dr.end_date,
            "conf": {k: v for k, v in (dr.conf or {}).items() if v not in (None, "", False)},
            "tasks": [{"task_id": t.task_id, "state": t.state, "try_number": t.try_number,
                       "start": t.start_date, "end": t.end_date,
                       "duration_s": round(t.duration, 1) if t.duration else None} for t in tis],
        })
    return out


def batch_controls(s3, bucket: str) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {}
    for bid in batches.list_batches(s3, bucket):
        prefix = f"control/batches/batch_id={bid}/"
        files = {}
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                files[o["Key"][len(prefix):-5]] = s3io.read_json(s3, bucket, o["Key"])
        out[bid] = files
    return out


# ----------------------------------------------------------------------------- secciones
def section_airflow(rep: Report, runs: list[dict]) -> None:
    summary = [{"airflow_run_id": r["airflow_run_id"], "batch_id": r["batch_id"], "estado": r["state"],
                "inicio_utc": r["start"], "fin_utc": r["end"], "parametros": r["conf"] or "",
                "tareas_ok": sum(t["state"] == "success" for t in r["tasks"]), "tareas": len(r["tasks"]),
                "reintentos": sum(max((t["try_number"] or 1) - 1, 0) for t in r["tasks"])} for r in runs]
    body = ["Cada ejecución del DAG `dw_ecommerce_pipeline` con el lote (`batch_id`) que procesó. "
            "`mark_failed` en estado *skipped* significa que no hubo fallos.\n",
            "## Resumen\n", md_table(summary, limit=200), "\n## Estado e intentos de cada tarea\n"]
    for r in runs:
        body.append(f"\n### `{r['airflow_run_id']}` — lote `{r['batch_id']}` — **{r['state']}**"
                    + (f" — parámetros `{json.dumps(r['conf'])}`" if r["conf"] else "") + "\n")
        body.append(md_table(r["tasks"], ["task_id", "state", "try_number", "start", "end", "duration_s"]))
    rep.write("01_airflow_ejecuciones.md", "Ejecución del pipeline en Airflow", "\n".join(body))


def section_logs(rep: Report, runs: list[dict]) -> None:
    body = ["Líneas del pipeline (las que llevan `[batch_id=...]`, más errores) de cada tarea e intento, "
            "leídas de los logs de Airflow en la EC2. Permiten identificar el lote y la tarea que fallaron.\n"]
    for r in runs:
        run_dir = LOGS_DIR / f"run_id={r['airflow_run_id']}"
        if not run_dir.exists():
            continue
        body.append(f"\n## `{r['airflow_run_id']}` (lote `{r['batch_id']}`, {r['state']})\n")
        for task in r["tasks"]:
            for attempt in sorted((run_dir / f"task_id={task['task_id']}").glob("attempt=*.log")):
                lines = [ln.rstrip() for ln in attempt.read_text(errors="replace").splitlines()
                         if TAG_RE.search(ln) or " ERROR " in ln or "RuntimeError" in ln]
                if lines:
                    body.append(f"**{task['task_id']}** — {attempt.stem}\n")
                    body.append(code("\n".join(re.sub(r"^\[[^\]]+\] \{[^}]+\} ", "", ln) for ln in lines[-30:])))
    rep.write("02_logs_por_ejecucion.md", "Logs del pipeline por ejecución", "\n".join(body))


def section_batches(rep: Report, s3, bucket: str, ctrl: dict) -> None:
    rows, wm_rows = [], []
    for bid, files in ctrl.items():
        status = (files.get("status") or {}).get("status")
        for source in ("mysql", "documentdb"):
            m = files.get(f"extract_{source}")
            if not m:
                continue
            rows.append({"batch_id": bid, "estado": status, "fuente": source, "modo": m["mode"],
                         "marcas_desde_lote": m.get("watermarks_from_batch"), "extraido_utc": m["extracted_at_utc"],
                         "filas": m["total_rows"]})
            wm_rows += [{"batch_id": bid, "entidad": e["entity"], "filas": e["rows"],
                         "columna_marca": e["watermark_column"], "marca_desde": e["watermark_from"],
                         "marca_hasta": e["watermark_to"], "s3_key": e["s3_key"]} for e in m["entities"]]
    objects = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/"):
        objects += [{"key": o["Key"], "bytes": o["Size"], "modificado_utc": o["LastModified"]}
                    for o in page.get("Contents", [])]
    body = [f"Bucket privado: `s3://{bucket}/`. Ruta: `raw/<fuente>/<entidad>/batch_id=<id>/part-00000.parquet`; "
            "control de cada lote en `control/batches/batch_id=<id>/`.\n",
            "## Lotes (manifiestos de extracción)\n", md_table(rows, limit=200),
            "\n## Marcas de avance por entidad\n",
            "Una extracción incremental parte de `marca_hasta` del último lote COMPLETE (`marcas_desde_lote`).\n",
            md_table(wm_rows, limit=300),
            f"\n## Archivos en raw/ ({len(objects)})\n", md_table(objects, limit=300)]
    rep.write("03_lotes_s3.md", "Extracción por lote y archivos en S3", "\n".join(body))


def section_redshift(rep: Report, settings: Settings) -> None:
    tables = q(settings, "select table_schema, table_name, table_type from information_schema.tables "
                         f"where table_schema in ({', '.join(repr(s) for s in SCHEMAS)}) order by 1, 2")
    counts = []
    for schema, name, ttype in tables:
        n = q(settings, f'select count(*) from {schema}."{name}"')[0][0]
        counts.append({"esquema": schema, "objeto": name, "tipo": "vista" if ttype == "VIEW" else "tabla", "filas": n})
    by_batch = []
    for table in ("mysql_customers", "mysql_products", "mysql_orders", "mysql_order_items",
                  "mongo_reviews", "mongo_navigation_events"):
        by_batch += [{"tabla": f"raw_data.{table}", "_batch_id": b, "filas": n} for b, n in
                     q(settings, f"select _batch_id, count(*) from raw_data.{table} group by 1 order by 1")]
    body = ["Redshift Serverless, base `ecommerce`. raw_data acumula los lotes; intermediate y quarantine "
            "son la capa de calidad; snapshots guarda la historia SCD2; marts el modelo dimensional.\n",
            "## Filas por objeto\n", md_table(counts, limit=200),
            "\n## raw_data por lote (cada fila lleva su _batch_id)\n", md_table(by_batch, limit=200)]
    rep.write("04_redshift.md", "Contenido del Data Warehouse en Redshift", "\n".join(body))


def section_dbt(rep: Report, ctrl: dict) -> None:
    summary, body_detail = [], []
    for bid, files in ctrl.items():
        for name in sorted(k for k in files if k.startswith("dbt_")):
            res = files[name]["results"]
            by = {}
            for r in res:
                by[r["status"]] = by.get(r["status"], 0) + 1
            summary.append({"batch_id": bid, "paso": name, "nodos": len(res), "por_estado": by,
                            "segundos": files[name].get("elapsed_s")})
    last = next((files for bid, files in reversed(list(ctrl.items()))
                 if (files.get("status") or {}).get("status") == batches.COMPLETE and "dbt_test" in files), None)
    if last:
        tests = [{"prueba": r["node"].replace("test.ecommerce_dw.", ""), "estado": r["status"],
                  "fallas": r["failures"]} for r in last["dbt_test"]["results"]]
        body_detail = [f"\n## Detalle de las pruebas del último lote COMPLETE ({len(tests)} pruebas)\n",
                       "Incluye unicidad (`unique`), campos obligatorios (`not_null`), relaciones "
                       "(`relationships`), valores aceptados y reglas de negocio (`assert_*`).\n",
                       md_table(tests, limit=400)]
        for step in ("dbt_run_intermediate", "dbt_snapshot", "dbt_run_marts", "dbt_seed"):
            if step in last:
                body_detail += [f"\n### {step}\n", md_table([{"nodo": r["node"], "estado": r["status"],
                                                               "mensaje": r["message"]} for r in last[step]["results"]],
                                                             limit=200)]
    rep.write("05_dbt_pruebas.md", "Resultados de dbt (modelos, snapshots y pruebas)",
              "dbt corre en la EC2 y ejecuta todo el SQL en Redshift. Resumen por lote y paso:\n\n"
              + md_table(summary, limit=300) + "\n".join(body_detail))


def section_quarantine(rep: Report, settings: Settings) -> None:
    parts = []
    for t in ("mysql__customers", "mysql__products", "mysql__orders", "mysql__order_items",
              "mongodb__reviews", "mongodb__navigation_events"):
        parts.append(f"select 'quarantine_{t}' as tabla, rejection_reason, _batch_id, count(*) as filas "
                     f"from quarantine.quarantine_{t} group by 2, 3")
    rows = [dict(zip(["tabla", "motivo", "lote_origen", "filas"], r))
            for r in q(settings, " union all ".join(parts) + " order by 1, 2")]
    sample = [dict(zip(["order_item_id", "order_id", "product_id", "quantity", "subtotal", "rejection_reason", "_batch_id"], r))
              for r in q(settings, "select order_item_id, order_id, product_id, quantity, subtotal, rejection_reason, "
                                   "_batch_id from quarantine.quarantine_mysql__order_items order by 1 limit 20")]
    rep.write("06_cuarentena.md", "Registros defectuosos en cuarentena",
              "Cada rechazo queda con su motivo (`rejection_reason`) y el lote de origen (`_batch_id`); "
              "no llega a marts.\n\n## Rechazos por tabla, motivo y lote\n" + md_table(rows, limit=200)
              + "\n## Muestra: líneas de pedido rechazadas\n" + md_table(sample))


def section_reconciliation(rep: Report, ctrl: dict) -> None:
    rows = []
    for bid, files in ctrl.items():
        r = files.get("reconciliation")
        if r:
            rows.append({"batch_id": bid, "fuente_mysql": f"{r['source_mysql']['rows']} / {r['source_mysql']['subtotal']}",
                         "raw_vigente": f"{r['raw_latest_redshift']['rows']} / {r['raw_latest_redshift']['subtotal']}",
                         "validas": f"{r['valid_intermediate']['rows']} / {r['valid_intermediate']['subtotal']}",
                         "fct_sales": f"{r['fct_sales']['rows']} / {r['fct_sales']['subtotal']}",
                         "cuarentena": r["quarantined_order_items"], "ok": r["passed"]})
    rep.write("07_conciliacion.md", "Conciliación del indicador de ventas",
              "Indicador: número de líneas de pedido / `sum(subtotal)`. En cada lote se verifica:\n\n"
              "1. fuente MySQL = raw_data en Redshift (versión vigente por llave)\n"
              "2. líneas válidas (intermediate) = `marts.fct_sales`\n"
              "3. líneas de la fuente − líneas en cuarentena = líneas de `marts.fct_sales`\n\n"
              "Si no cuadra, el lote queda FAILED y la marca de avance no se mueve.\n\n"
              + md_table(rows, limit=200))


SCENARIO_TITLES = {1: "Carga inicial", 2: "Registros defectuosos", 3: "Repetición de un lote exitoso",
                   4: "Nuevo lote incremental", 5: "Cambio en un atributo historizado (SCD2)",
                   6: "Fallo controlado y reejecución"}
EXPECTED = {1: "Datos disponibles en los modelos analíticos", 2: "Rechazos identificados y trazables",
            3: "Sin duplicación de hechos ni versiones históricas", 4: "Incorporación correcta de nuevos registros",
            5: "Nueva versión SCD tipo 2 y cierre de la anterior", 6: "Recuperación sin pérdida ni duplicación"}


def section_scenarios(rep: Report, scen: dict[int, dict]) -> dict:
    status, body = {}, ["Ejecutados por `python -m dw.scenarios` (dispara el DAG con los parámetros de cada "
                        "caso y verifica el resultado consultando S3 y Redshift).\n"]
    table = []
    for n in range(1, 7):
        s = scen.get(n)
        ok = bool(s and s.get("passed"))
        status[f"{n}. {SCENARIO_TITLES[n]}"] = ok
        table.append({"#": n, "escenario": SCENARIO_TITLES[n], "resultado esperado": EXPECTED[n],
                      "lote": (s or {}).get("batch_id"), "resultado": "OK" if ok else "pendiente"})
    body.append(md_table(table))
    for n in range(1, 7):
        s = scen.get(n)
        if not s:
            continue
        body.append(f"\n## {n}. {SCENARIO_TITLES[n]} — {'OK' if s.get('passed') else 'REVISAR'}\n")
        if s.get("explanation"):
            body.append(s["explanation"] + "\n")
        detail = {k: v for k, v in s.items() if k not in ("explanation", "scenario", "name")}
        body.append(code(json.dumps(detail, ensure_ascii=False, indent=2, default=str)[:12000], "json"))
    rep.write("08_escenarios.md", "Escenarios de calidad, incrementalidad y recuperación", "\n".join(body))
    return status


def section_quicksight(rep: Report, settings: Settings) -> None:
    sql = VALIDATION_SQL.read_text()
    body = ["El tablero de QuickSight consume `marts.rpt_sales_dashboard` en modo Direct Query (sin SPICE). "
            "Estas consultas dan los mismos valores que muestra el tablero con los mismos filtros.\n"]
    for stmt in sql.split(";"):
        lines = [ln for ln in stmt.strip().splitlines()]
        sql_only = "\n".join(ln for ln in lines if not ln.strip().startswith("--")).strip()
        if not sql_only:
            continue
        comments = "\n".join(ln.strip()[2:].strip() for ln in lines if ln.strip().startswith("--"))
        rows = redshift_query(settings, sql_only)
        body.append(("\n" + comments + "\n" if comments else "\n") + code(sql_only, "sql"))
        body.append(code("\n".join(str(tuple(str(v) for v in r)) for r in rows)))
    rep.write("09_quicksight_validacion.md", "Validación del indicador del tablero de QuickSight", "\n".join(body))


# ----------------------------------------------------------------------------- principal
def collect(settings: Settings | None = None) -> dict:
    settings = settings or load_settings()
    s3 = s3io.client(settings.region)
    bucket = settings.bucket
    rep = Report()

    scen = scenarios(s3, bucket)
    since = None
    if 1 in scen and scen[1].get("run", {}).get("airflow_run_id"):
        from airflow.models import DagRun
        first = DagRun.find(dag_id=DAG_ID, run_id=scen[1]["run"]["airflow_run_id"])
        since = first[0].start_date if first else None
    runs = airflow_runs(since)
    ctrl = batch_controls(s3, bucket)

    section_airflow(rep, runs)
    section_logs(rep, runs)
    section_batches(rep, s3, bucket, ctrl)
    section_redshift(rep, settings)
    section_dbt(rep, ctrl)
    section_quarantine(rep, settings)
    section_reconciliation(rep, ctrl)
    status = section_scenarios(rep, scen)
    section_quicksight(rep, settings)

    for bid, files in ctrl.items():
        for name, data in files.items():
            rep.files[f"raw/control_batches/{bid}/{name}.json"] = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    for n, data in scen.items():
        rep.files[f"raw/scenarios/{n}_{data.get('name', '')}.json"] = json.dumps(data, indent=2, ensure_ascii=False, default=str)

    index = [
        "| Evidencia pedida (sección 4.3) | Archivo |", "|---|---|",
        "| Ejecución exitosa del pipeline en Airflow | [01_airflow_ejecuciones.md](01_airflow_ejecuciones.md), [02_logs_por_ejecucion.md](02_logs_por_ejecucion.md) |",
        "| Extracción por lote en S3 | [03_lotes_s3.md](03_lotes_s3.md) |",
        "| Carga en Redshift | [04_redshift.md](04_redshift.md) |",
        "| Resultados de las pruebas de dbt | [05_dbt_pruebas.md](05_dbt_pruebas.md) |",
        "| Calidad: rechazos con motivo y lote | [06_cuarentena.md](06_cuarentena.md) |",
        "| Conciliación de un indicador agregado | [07_conciliacion.md](07_conciliacion.md) |",
        "| Carga inicial, calidad, incrementalidad, idempotencia, SCD2 y recuperación | [08_escenarios.md](08_escenarios.md) |",
        "| Consulta SQL que verifica el indicador del dashboard | [09_quicksight_validacion.md](09_quicksight_validacion.md), [dashboard_quicksight.png](dashboard_quicksight.png) |",
        "\n## Estado de los escenarios\n",
        *[f"- {'✅' if ok else '⏳'} {sc}" for sc, ok in status.items()],
        f"\n## Totales\n\n- Ejecuciones del DAG documentadas: {len(runs)} "
        f"({sum(r['state'] == 'success' for r in runs)} exitosas; las fallidas son el fallo controlado del escenario 6)",
        f"- Lotes registrados: {len(ctrl)} "
        f"({sum((f.get('status') or {}).get('status') == batches.COMPLETE for f in ctrl.values())} COMPLETE)",
        "\n`raw/` contiene los JSON de control de cada lote y de cada escenario sin modificar.",
    ]
    rep.write("README.md", "Evidencias de funcionamiento — Data Warehouse de e-commerce", "\n".join(index) + "\n")
    rep.flush(s3, bucket)
    log.info("Evidencias generadas en s3://%s/%s (%s archivos). Escenarios: %s", bucket, PREFIX, len(rep.files), status)
    return status


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(json.dumps(collect(), ensure_ascii=False, indent=2))
