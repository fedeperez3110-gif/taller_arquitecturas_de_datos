"""Deja el Data Warehouse en blanco para repetir los escenarios desde cero.

Borra en Redshift los esquemas que crean la carga y dbt, y en S3 los prefijos raw/ y control/.
Las fuentes (MySQL y DocumentDB) las reinicia generate_data en el escenario 1.

    docker compose exec -T airflow-scheduler python -m dw.reset --yes
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

from dw import s3io
from dw.connections import redshift_connect
from dw.settings import load_settings

SCHEMAS = ["marts", "snapshots", "quarantine", "intermediate", "reference", "raw_data"]
PREFIXES = ["raw/", "control/"]
LOCAL = Path("/opt/airflow/dw/evidencias/scenarios")
log = logging.getLogger("dw.reset")


def reset(settings) -> None:
    conn = redshift_connect(settings)
    try:
        cur = conn.cursor()
        for schema in SCHEMAS:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            log.info("Redshift: esquema %s eliminado", schema)
        conn.commit()
    finally:
        conn.close()
    s3 = s3io.client(settings.region)
    for prefix in PREFIXES:
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=settings.bucket, Prefix=prefix):
            keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=settings.bucket, Delete={"Objects": keys[i:i + 1000]})
        log.info("S3: %s objetos borrados bajo %s", len(keys), prefix)
    if LOCAL.exists():
        shutil.rmtree(LOCAL)
        log.info("Evidencias locales anteriores borradas")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="Confirma el borrado")
    if not parser.parse_args().yes:
        print("Agrega --yes para confirmar: borra raw_data, intermediate, quarantine, snapshots, marts y raw/ + control/ en S3")
        return 1
    reset(load_settings())
    return 0


if __name__ == "__main__":
    sys.exit(main())
