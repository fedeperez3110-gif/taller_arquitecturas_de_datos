"""Validación previa de parámetros y conexiones (requisito 3.4.1).

Falla rápido, antes de extraer nada, si falta una variable, un secreto o alguna conexión.
"""
from __future__ import annotations

import logging
from pathlib import Path

from dw import s3io
from dw.connections import docdb_client, mysql_connect, redshift_query
from dw.settings import Settings, get_secret

log = logging.getLogger(__name__)


def validate(settings: Settings, batch_id: str, params: dict) -> dict:
    ctx = f"[batch_id={batch_id}][task=validate_connections]"
    if params.get("extraction_mode") not in ("auto", "full"):
        raise ValueError(f"{ctx} extraction_mode debe ser auto o full")
    checks = {}
    s3io.client(settings.region).head_bucket(Bucket=settings.bucket)
    checks["s3_bucket"] = settings.bucket
    for name in (settings.mysql_secret, settings.docdb_secret, settings.redshift_secret):
        secret = get_secret(name, settings.region)
        missing = {"host", "port", "username", "password", "database"} - set(secret)
        if missing:
            raise ValueError(f"{ctx} al secreto {name} le faltan {sorted(missing)}")
    checks["secrets"] = "ok"
    if not Path(settings.ca_bundle).exists():
        raise FileNotFoundError(f"{ctx} no existe el certificado {settings.ca_bundle}")
    conn = mysql_connect(settings)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            checks["mysql"] = cur.fetchone()[0]
    finally:
        conn.close()
    client = docdb_client(settings)
    try:
        checks["documentdb"] = client.server_info().get("version")
    finally:
        client.close()
    checks["redshift"] = redshift_query(settings, "select version()")[0][0][:60]
    if not Path(settings.dbt_executable).exists():
        raise FileNotFoundError(f"{ctx} no existe dbt en {settings.dbt_executable} (reconstruye la imagen)")
    checks["dbt"] = settings.dbt_executable
    log.info("%s conexiones OK: %s", ctx, checks)
    return checks
