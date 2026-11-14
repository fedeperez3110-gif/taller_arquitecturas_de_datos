"""Ejecuta dbt (en su propio entorno virtual de la imagen) contra Redshift.

Las credenciales se pasan como variables de entorno al proceso de dbt (profiles.yml las lee con
env_var); no se escriben en disco. El resultado (run_results.json) se guarda en S3 por lote como
evidencia de las pruebas.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

from dw import batches, s3io
from dw.settings import Settings, get_secret

log = logging.getLogger(__name__)


def dbt_env(settings: Settings) -> dict:
    s = get_secret(settings.redshift_secret, settings.region)
    return {
        **os.environ,
        "DBT_REDSHIFT_HOST": s["host"], "DBT_REDSHIFT_PORT": str(s["port"]),
        "DBT_REDSHIFT_USER": s["username"], "DBT_REDSHIFT_PASSWORD": s["password"],
        "DBT_REDSHIFT_DATABASE": s["database"],
        "DBT_PROFILES_DIR": settings.dbt_project_dir,
    }


def run_dbt(settings: Settings, batch_id: str, command: str, selector: str | None = None,
            extra: list[str] | None = None, label: str | None = None) -> dict | None:
    args = [settings.dbt_executable, command, "--project-dir", settings.dbt_project_dir,
            "--profiles-dir", settings.dbt_project_dir]
    if selector:
        args += ["--select", *selector.split()]
    args += extra or []
    target = Path(settings.dbt_project_dir) / "target"
    results_file = target / "run_results.json"
    if results_file.exists():
        results_file.unlink()   # que no se reporte el resultado de un comando anterior
    log.info("[batch_id=%s][dbt=%s] %s", batch_id, command, " ".join(args[1:]))
    proc = subprocess.run(args, env=dbt_env(settings), capture_output=True, text=True)
    for line in proc.stdout.splitlines():
        log.info("[batch_id=%s][dbt=%s] %s", batch_id, command, line)
    if proc.stderr.strip():
        log.warning("[batch_id=%s][dbt=%s] stderr: %s", batch_id, command, proc.stderr.strip()[-2000:])

    summary = None
    if command != "deps" and results_file.exists():
        data = json.loads(results_file.read_text())
        summary = {
            "command": command, "selector": selector, "returncode": proc.returncode,
            "elapsed_s": round(data.get("elapsed_time", 0), 1),
            "results": [{"node": r["unique_id"], "status": r["status"],
                         "failures": r.get("failures"), "message": (r.get("message") or "")[:300]}
                        for r in data.get("results", [])],
        }
        name = f"dbt_{label or command}"
        s3io.put_json(s3io.client(settings.region), settings.bucket, batches.control_key(batch_id, name), summary)
    if proc.returncode != 0:
        raise RuntimeError(f"[batch_id={batch_id}] dbt {command} falló (código {proc.returncode}); ver log")
    return summary
