"""Recolector automático de evidencias para la entrega (sección 5.3 del enunciado).

Se ejecuta DENTRO del contenedor de Airflow en la EC2 (tiene acceso a la base de Airflow, a los
logs y a AWS con el rol IAM) y genera archivos Markdown en  s3://<bucket>/evidence/ :

  README.md                       índice y resumen
  01_airflow_ejecuciones.md       ejecuciones del DAG, parámetros, estado e intentos de cada tarea
  02_logs_por_ejecucion.md        líneas de log del pipeline por ejecución, tarea e intento
  03_archivos_s3.md               archivos de Bronze, Silver, Gold, cuarentena y control + muestras
  04_comprehend.md                resumen por lote, ejemplo de respuesta en caché, entidades principales
  05_glue_catalog.md              tablas, columnas, ubicación y particiones registradas
  06_athena_consultas.md          resultado de sql/01–07 con bytes escaneados
  07_validaciones.md              validaciones v01–v10 de cada lote
  08_escenarios.md                verificación automática de los 8 escenarios de la sección 4.5
  raw/                            JSON de control/ copiados tal cual

Uso (en la EC2):   ./infra/collect_evidence.sh   (desde tu computador; lo ejecuta y lo descarga)
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.athena import VALIDATIONS_DIR, run_query
from datalake.config import Settings, load_settings
from datalake.silver import load_current_news

log = logging.getLogger(__name__)

PREFIX = "evidence/"
SQL_DIR = Path("/opt/airflow/sql")
LOGS_DIR = Path("/opt/airflow/logs/dag_id=rss_datalake")
DAG_ID = "rss_datalake"
MAX_ROWS = 25
TAG_RE = re.compile(r"\[run_id=\d{8}T\d{6}Z\]")


# ============================================================================ utilidades Markdown
def md_table(rows: list[dict], columns: list[str] | None = None, limit: int = MAX_ROWS) -> str:
    if not rows:
        return "_(sin filas)_\n"
    columns = columns or list(rows[0].keys())

    def cell(v):
        if isinstance(v, (list, tuple)):
            v = ", ".join(map(str, v))
        if isinstance(v, datetime):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        return str(v if v is not None else "").replace("|", "\\|").replace("\n", " ")[:120]

    out = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    out += ["| " + " | ".join(cell(r.get(c)) for c in columns) + " |" for r in rows[:limit]]
    if len(rows) > limit:
        out.append(f"\n_… {len(rows) - limit} filas más (se muestran {limit})._")
    return "\n".join(out) + "\n"


def code(text: str, lang: str = "") -> str:
    return f"```{lang}\n{text.rstrip()}\n```\n"


class Report:
    def __init__(self, s3, bucket: str):
        self.s3, self.bucket = s3, bucket
        self.files: dict[str, str] = {}

    def write(self, name: str, title: str, body: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        self.files[name] = f"# {title}\n\n_Generado automáticamente el {stamp} por `src/datalake/evidence.py`._\n\n{body}"

    def flush(self) -> None:
        for old in s3_utils.list_keys(self.s3, self.bucket, PREFIX):   # el reporte se regenera completo
            s3_utils.delete_key(self.s3, self.bucket, old)
        for name, text in self.files.items():
            ctype = "application/json" if name.endswith(".json") else "text/markdown; charset=utf-8"
            s3_utils.put_bytes(self.s3, self.bucket, PREFIX + name, text.encode("utf-8"), ctype)


def control_runs(s3, bucket: str) -> dict[str, dict[str, dict]]:
    """{run_id: {etapa: json}} a partir de control/runs/."""
    runs: dict[str, dict[str, dict]] = defaultdict(dict)
    for key in s3_utils.list_keys(s3, bucket, "control/runs/"):
        m = re.search(r"run_id=(\d{8}T\d{6}Z)/(\w+)\.json$", key)
        if m:
            runs[m.group(1)][m.group(2)] = s3_utils.read_json(s3, bucket, key)
    return dict(sorted(runs.items()))


# ============================================================================ 01 Airflow
def airflow_runs() -> list[dict]:
    from airflow.models import DagRun

    out = []
    for dr in sorted(DagRun.find(dag_id=DAG_ID), key=lambda d: d.start_date or d.execution_date):
        tis = sorted(dr.get_task_instances(), key=lambda t: t.start_date or dr.start_date)
        out.append({
            "airflow_run_id": dr.run_id, "state": dr.state, "run_type": dr.run_type,
            "start": dr.start_date, "end": dr.end_date,
            "conf": {k: v for k, v in (dr.conf or {}).items() if v},
            "tasks": [{"task_id": t.task_id, "state": t.state, "try_number": t.try_number,
                       "start": t.start_date, "end": t.end_date,
                       "duration_s": round(t.duration, 1) if t.duration else None} for t in tis],
        })
    return out


def section_airflow(rep: Report, runs: list[dict]) -> None:
    summary = [{"airflow_run_id": r["airflow_run_id"], "tipo": r["run_type"], "estado": r["state"],
                "inicio_utc": r["start"], "fin_utc": r["end"], "parametros": json.dumps(r["conf"]) if r["conf"] else "",
                "tareas_ok": sum(t["state"] == "success" for t in r["tasks"]), "tareas": len(r["tasks"]),
                "reintentos": sum(max((t["try_number"] or 1) - 1, 0) for t in r["tasks"])} for r in runs]
    body = ["## Resumen de ejecuciones\n", md_table(summary, limit=200)]
    body.append("\n## Detalle por ejecución (estado e intentos de cada tarea)\n")
    for r in runs:
        body.append(f"\n### `{r['airflow_run_id']}` — {r['state']}"
                    + (f" — parámetros `{json.dumps(r['conf'])}`" if r["conf"] else "") + "\n")
        body.append(md_table(r["tasks"], ["task_id", "state", "try_number", "start", "end", "duration_s"]))
    rep.write("01_airflow_ejecuciones.md", "Ejecución del DAG en Airflow", "\n".join(body))


# ============================================================================ 02 Logs
def section_logs(rep: Report, runs: list[dict]) -> None:
    body = ["Líneas del pipeline (las que llevan `[run_id=...]`, más errores) de cada tarea e intento, "
            "leídas de los logs de Airflow en la EC2.\n"]
    for r in runs:
        run_dir = LOGS_DIR / f"run_id={r['airflow_run_id']}"
        if not run_dir.exists():
            continue
        body.append(f"\n## `{r['airflow_run_id']}`\n")
        for task in r["tasks"]:
            for attempt in sorted((run_dir / f"task_id={task['task_id']}").glob("attempt=*.log")):
                lines = [ln.rstrip() for ln in attempt.read_text(errors="replace").splitlines()
                         if TAG_RE.search(ln) or " ERROR " in ln or "AirflowException" in ln]
                if lines:
                    body.append(f"**{task['task_id']}** — {attempt.stem}\n")
                    body.append(code("\n".join(re.sub(r"^\[[^\]]+\] \{[^}]+\} ", "", ln) for ln in lines[-40:])))
    rep.write("02_logs_por_ejecucion.md", "Logs del pipeline por ejecución", "\n".join(body))


# ============================================================================ 03 S3
def section_s3(rep: Report, s3, bucket: str) -> None:
    body = [f"Bucket único: `s3://{bucket}/`\n"]
    layers = [("Bronze", "bronze/"), ("Silver", "silver/"), ("Gold", "gold/"),
              ("Cuarentena", "quarantine/"), ("Caché de Comprehend", "cache/comprehend/"),
              ("Control", "control/")]
    for name, prefix in layers:
        objs = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            objs += [{"key": o["Key"], "bytes": o["Size"], "modificado_utc": o["LastModified"]}
                     for o in page.get("Contents", [])]
        body.append(f"\n## {name} (`{prefix}`) — {len(objs)} archivos, "
                    f"{sum(o['bytes'] for o in objs):,} bytes\n")
        body.append(md_table(objs, limit=60))

    manifests = [k for k in s3_utils.list_keys(s3, bucket, "bronze/") if k.endswith("manifest.json")]
    if manifests:
        body.append(f"\n## Ejemplo de `manifest.json` de Bronze\n\n`{manifests[-1]}`\n")
        body.append(code(json.dumps(s3_utils.read_json(s3, bucket, manifests[-1]), indent=2, ensure_ascii=False), "json"))
    news = sorted(load_current_news(s3, bucket).values(), key=lambda n: n["news_id"])
    body.append(f"\n## Muestra de Silver (`silver_news`, {len(news)} noticias vigentes)\n")
    body.append(md_table(news, ["news_id", "published_date", "sections", "title", "pub_date_status",
                                "version", "first_seen_run_id", "content_run_id"], limit=10))
    gold_key = next((k for k in s3_utils.list_keys(s3, bucket, "gold/entity_daily/") if k.endswith(".parquet")), None)
    if gold_key:
        rows = sorted(pio.read_table(s3, bucket, gold_key).to_pylist(), key=lambda r: -r["news_count"])
        body.append(f"\n## Muestra de Gold (`{gold_key}`)\n")
        body.append(md_table(rows, limit=10))
    rep.write("03_archivos_s3.md", "Archivos de Bronze, Silver y Gold", "\n".join(body))


# ============================================================================ 04 Comprehend
def section_comprehend(rep: Report, s3, bucket: str, ctrl: dict) -> None:
    rows = [{"run_id": rid, **{k: st["comprehend"].get(k) for k in (
        "news_current", "texts_analyzed", "texts_from_cache", "texts_failed", "texts_truncated",
        "entity_rows", "key_phrase_rows")}} for rid, st in ctrl.items() if "comprehend" in st]
    body = ["## Llamadas a Comprehend por lote\n",
            "`texts_analyzed` = textos enviados a Comprehend (lo que se paga); `texts_from_cache` = "
            "noticias cuyo texto no cambió y se tomaron de la caché.\n\n", md_table(rows, limit=200)]
    cache_keys = s3_utils.list_keys(s3, bucket, "cache/comprehend/")
    if cache_keys:
        entry = s3_utils.read_json(s3, bucket, cache_keys[0])
        entry["entities"] = entry.get("entities", [])[:8]
        entry["key_phrases"] = entry.get("key_phrases", [])[:8]
        body.append(f"\n## Ejemplo de respuesta en caché (`{cache_keys[0]}`, recortada a 8 elementos)\n")
        body.append(code(json.dumps(entry, indent=2, ensure_ascii=False), "json"))
    ents = pio.read_dataset(s3, bucket, "silver/news_entities/", pio.ENTITY_SCHEMA)
    by_type: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    texts: dict[tuple, str] = {}
    for e in ents:
        by_type[e["entity_type"]][e["entity_normalized"]].add(e["news_id"])
        texts[(e["entity_type"], e["entity_normalized"])] = e["entity_text"]
    for etype in ("PERSON", "ORGANIZATION", "LOCATION"):
        top = sorted(by_type[etype].items(), key=lambda kv: (-len(kv[1]), kv[0]))[:10]
        body.append(f"\n## {etype}: entidades en más noticias (Silver)\n")
        body.append(md_table([{"entidad": texts[(etype, k)], "normalizada": k, "noticias": len(v)} for k, v in top]))
    rep.write("04_comprehend.md", "Resultados de Amazon Comprehend", "\n".join(body))


# ============================================================================ 05 Glue
def section_glue(rep: Report, glue, settings: Settings) -> None:
    body = [f"Base de datos: `{settings.glue_database}`\n"]
    for page in glue.get_paginator("get_tables").paginate(DatabaseName=settings.glue_database):
        for t in sorted(page["TableList"], key=lambda t: t["Name"]):
            sd = t["StorageDescriptor"]
            parts = []
            for pp in glue.get_paginator("get_partitions").paginate(DatabaseName=settings.glue_database,
                                                                   TableName=t["Name"]):
                parts += [{"valor": p["Values"][0], "ubicacion": p["StorageDescriptor"]["Location"],
                           "creada_utc": p.get("CreationTime")} for p in pp["Partitions"]]
            body.append(f"\n## `{t['Name']}`\n\n{t.get('Description', '')}\n\n"
                        f"- Ubicación: `{sd['Location']}`\n- Formato: `{sd['SerdeInfo']['SerializationLibrary']}`\n"
                        f"- Partición: `{t['PartitionKeys'][0]['Name']}` — {len(parts)} registradas\n\n")
            body.append("<details><summary>Columnas</summary>\n\n"
                        + md_table([{"columna": c["Name"], "tipo": c["Type"]} for c in sd["Columns"]], limit=100)
                        + "\n</details>\n\n")
            body.append(md_table(sorted(parts, key=lambda p: p["valor"]), limit=100))
    rep.write("05_glue_catalog.md", "Tablas y particiones en Glue Data Catalog", "\n".join(body))


# ============================================================================ 06 Athena
def split_statements(sql: str) -> list[tuple[str, str]]:
    """Separa un archivo .sql en sentencias, conservando el comentario que las precede como título."""
    out = []
    for chunk in sql.split(";"):
        lines = chunk.strip().splitlines()
        comments = [ln.strip("- ").strip() for ln in lines if ln.strip().startswith("--")]
        stmt = "\n".join(ln for ln in lines if not ln.strip().startswith("--")).strip()
        if stmt:
            out.append((comments[0] if comments else "", stmt))
    return out


def section_athena(rep: Report, athena, settings: Settings) -> list[dict]:
    body = [f"Workgroup `{settings.athena_workgroup}`, base `{settings.glue_database}`. "
            "Consultas tomadas de `sql/`. En `03` se usa la persona con más noticias.\n"]
    top_rows, _ = run_query(athena, settings,
                            "SELECT entity_normalized FROM gold_entity_daily WHERE entity_type = 'PERSON' "
                            "GROUP BY 1 ORDER BY sum(news_count) DESC LIMIT 1")
    top_person = top_rows[0]["entity_normalized"] if top_rows else "petro"
    stats_all = []
    for path in sorted(SQL_DIR.glob("*.sql")):
        body.append(f"\n## `sql/{path.name}`\n")
        sql = path.read_text(encoding="utf-8").replace("'petro'", f"'{top_person}'")
        for title, stmt in split_statements(sql):
            rows, stats = run_query(athena, settings, stmt)
            stats_all.append({"consulta": f"{path.stem}: {title[:60]}", "bytes_escaneados": stats["bytes_scanned"],
                              "ms": stats["elapsed_ms"], "filas": len(rows), "query_id": stats["query_id"]})
            body.append(f"\n**{title}**\n\n" + code(stmt, "sql")
                        + f"\nBytes escaneados: **{stats['bytes_scanned']:,}** · "
                          f"tiempo: {stats['elapsed_ms']} ms · filas: {len(rows)} · id: `{stats['query_id']}`\n\n"
                        + md_table(rows))
    body.insert(1, "\n## Resumen de bytes escaneados\n\n" + md_table(stats_all, limit=100))
    rep.write("06_athena_consultas.md", "Consultas y resultados en Athena", "\n".join(body))
    return stats_all


# ============================================================================ 07 Validaciones
def section_validations(rep: Report, ctrl: dict) -> None:
    checks = sorted(p.stem for p in VALIDATIONS_DIR.glob("v*.sql"))
    rows = []
    for rid, st in ctrl.items():
        v = st.get("validations")
        if v:
            by = {c["check"]: c["violations"] for c in v["checks"]}
            rows.append({"run_id": rid, "resultado": "OK" if v["passed"] else "FALLA",
                         **{c.split("_")[0]: by.get(c, "") for c in checks}})
    body = ["Cada validación devuelve el número de violaciones (0 = correcto). Definiciones en "
            "`src/datalake/sql_validations/`.\n\n"]
    body.append(md_table([{"id": c.split("_")[0], "validación": c} for c in checks], limit=50))
    body.append("\n## Resultado por lote\n\n" + md_table(rows, limit=200))
    rep.write("07_validaciones.md", "Validaciones de calidad por lote", "\n".join(body))


# ============================================================================ 08 Escenarios
def section_scenarios(rep: Report, s3, bucket: str, ctrl: dict, runs: list[dict], bytes_stats: list[dict]) -> dict:
    news = load_current_news(s3, bucket)
    final_by_airflow = {st["final"].get("airflow_run_id"): rid for rid, st in ctrl.items() if "final" in st}

    def silver(rid):
        return ctrl.get(rid, {}).get("silver", {}).get("totals", {})

    def comp(rid):
        return ctrl.get(rid, {}).get("comprehend", {})

    found: dict[str, list[str]] = defaultdict(list)
    first_catalog_seen = False      # el primer registro en Glue da de alta TODO: no es "nueva partición"
    for r in runs:
        rid = final_by_airflow.get(r["airflow_run_id"])
        if not rid or r["state"] != "success":
            continue
        t, c = silver(rid), comp(rid)
        reproc = r["conf"].get("reprocess_run_id")
        controlled = ctrl.get(rid, {}).get("bronze", {}).get("controlled_copy_of")
        retried = [x["task_id"] for x in r["tasks"] if (x["try_number"] or 1) > 1]
        line = (f"`{r['airflow_run_id']}` → lote `{rid}`: nuevos={t.get('new')}, actualizados={t.get('updated')}, "
                f"sin_cambio={t.get('unchanged')}, rechazados={t.get('rejected')}, "
                f"Comprehend analizados={c.get('texts_analyzed')} / caché={c.get('texts_from_cache')}")
        if not reproc and t.get("new"):
            found["Carga inicial" if not found["Carga inicial"] else "Nuevo lote incremental"].append(line)
        if reproc and not controlled:
            ok = not t.get("new") and not t.get("updated") and not c.get("texts_analyzed")
            found["Repetición de un lote exitoso"].append(line + (" ✅" if ok else " ⚠️"))
            found["Reprocesamiento desde Bronze"].append(line + " (sin consultar los RSS)")
        if t.get("updated") and not controlled and not reproc:
            # Cambio real publicado por el medio entre dos lecturas del RSS: también demuestra el escenario.
            found["Noticia con contenido modificado"].append(line + " (cambio real en el RSS; nueva versión en Silver)")
        if controlled:
            if t.get("updated"):
                found["Noticia con contenido modificado"].append(line + f" (copia controlada de `{controlled}`)")
            if t.get("rejected"):
                found["Registros defectuosos"].append(line + f" (copia controlada de `{controlled}`)")
        if r["conf"].get("simulate_failure") and retried:
            found["Fallo controlado y reejecución"].append(
                f"`{r['airflow_run_id']}`: `{r['conf']['simulate_failure']}` falló en el intento 1 y terminó en "
                f"`success` en el intento {max(x['try_number'] for x in r['tasks'])}; tareas reintentadas: {retried}; "
                f"validaciones del lote: {'OK' if ctrl.get(rid, {}).get('validations', {}).get('passed') else 'revisar'}")
        cat = ctrl.get(rid, {}).get("catalog", {})
        added = sorted({p for tb in cat.get("tables", []) for p in tb.get("added", [])
                        if tb["table"] == "silver_news"})
        if cat:
            if first_catalog_seen and added:
                found["Nueva partición"].append(
                    f"`{rid}` registró en Glue `silver_news` las particiones nuevas {added} "
                    f"(consultables en Athena al terminar el lote)")
            first_catalog_seen = True

    order = ["Carga inicial", "Registros defectuosos", "Repetición de un lote exitoso", "Nuevo lote incremental",
             "Noticia con contenido modificado", "Fallo controlado y reejecución", "Reprocesamiento desde Bronze",
             "Nueva partición"]
    body = ["Detección automática a partir de los metadatos de Airflow y de `control/runs/`. "
            "Cada línea es una ejecución real que demuestra el escenario.\n"]
    status = {}
    for sc in order:
        status[sc] = bool(found[sc])
        body.append(f"\n## {'✅' if found[sc] else '⏳'} {sc}\n")
        body.append("\n".join(f"- {x}" for x in found[sc]) + "\n" if found[sc]
                    else "_Aún no hay una ejecución que lo demuestre (ver README › Cómo demostrar los escenarios)._\n")

    updated = [n for n in news.values() if n["version"] > 1]
    if updated:
        body.append("\n### Noticias con más de una versión\n")
        body.append(md_table(updated, ["news_id", "version", "title", "first_seen_run_id", "content_run_id"]))
    q_rows = pio.read_dataset(s3, bucket, "quarantine/silver_rejected/", pio.QUARANTINE_SCHEMA)
    if q_rows:
        body.append("\n### Registros en cuarentena\n")
        body.append(md_table(q_rows, ["run_id", "source_id", "item_position", "reasons", "title_raw", "link", "bronze_key"]))
    part = [b for b in bytes_stats if b["consulta"].startswith("06_")]
    if part:
        body.append("\n### Bytes escaneados con y sin filtro de partición (`sql/06`)\n")
        body.append(md_table(part))
    rep.write("08_escenarios.md", "Validación de los escenarios de la sección 4.5", "\n".join(body))
    return status


# ============================================================================ orquestación
def collect(settings: Settings | None = None) -> dict:
    settings = settings or load_settings()
    s3 = s3_utils.s3_client(settings.region)
    glue = boto3.client("glue", region_name=settings.region)
    athena = boto3.client("athena", region_name=settings.region)
    bucket = settings.bucket
    rep = Report(s3, bucket)

    ctrl = control_runs(s3, bucket)
    runs = airflow_runs()
    section_airflow(rep, runs)
    section_logs(rep, runs)
    section_s3(rep, s3, bucket)
    section_comprehend(rep, s3, bucket, ctrl)
    section_glue(rep, glue, settings)
    bytes_stats = section_athena(rep, athena, settings)
    section_validations(rep, ctrl)
    status = section_scenarios(rep, s3, bucket, ctrl, runs, bytes_stats)

    for rid, stages in ctrl.items():
        for stage, data in stages.items():
            rep.files[f"raw/control_runs/{rid}/{stage}.json"] = json.dumps(data, indent=2, ensure_ascii=False)

    index = [
        "| Evidencia pedida (sección 5.3) | Archivo |", "|---|---|",
        "| Ejecución del DAG en Airflow | [01_airflow_ejecuciones.md](01_airflow_ejecuciones.md), [02_logs_por_ejecucion.md](02_logs_por_ejecucion.md) |",
        "| Archivos de Bronze, Silver y Gold | [03_archivos_s3.md](03_archivos_s3.md) |",
        "| Resultados de Comprehend | [04_comprehend.md](04_comprehend.md) |",
        "| Tablas y particiones en Glue Data Catalog | [05_glue_catalog.md](05_glue_catalog.md) |",
        "| Consultas y resultados en Athena | [06_athena_consultas.md](06_athena_consultas.md) |",
        "| Validación de calidad, incrementalidad y recuperación | [07_validaciones.md](07_validaciones.md), [08_escenarios.md](08_escenarios.md) |",
        "\n## Estado de los escenarios\n",
        *[f"- {'✅' if ok else '⏳'} {sc}" for sc, ok in status.items()],
        f"\n## Totales\n\n- Ejecuciones del DAG: {len(runs)} ({sum(r['state'] == 'success' for r in runs)} exitosas)",
        f"- Lotes con registro de control: {len(ctrl)}",
        f"- Noticias vigentes en Silver: {len(load_current_news(s3, bucket))}",
        "\n`raw/control_runs/` contiene los JSON de control de cada lote sin modificar.",
    ]
    rep.write("README.md", "Evidencias de funcionamiento — Data Lake de noticias RSS", "\n".join(index) + "\n")
    rep.flush()
    log.info("Evidencias generadas en s3://%s/%s (%s archivos). Escenarios: %s",
             bucket, PREFIX, len(rep.files), status)
    return status


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(json.dumps(collect(), ensure_ascii=False, indent=2))
