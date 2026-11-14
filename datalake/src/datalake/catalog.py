"""Sincroniza las particiones del Glue Data Catalog con lo que existe en S3.

Las tablas se definen en Terraform (infra/terraform/glue_tables.tf). En cada lote, después de
escribir Silver y Gold, esta tarea:
  * registra en Glue las particiones nuevas que aparecieron en S3 (BatchCreatePartition), y
  * elimina del catálogo las que ya no tienen archivos (BatchDeletePartition).
Así una partición nueva (p. ej. un día nuevo) queda consultable en Athena en cuanto termina el
lote, sin crawlers ni MSCK REPAIR TABLE.
"""
from __future__ import annotations

import copy
import logging

import boto3

from datalake import parquet_io as pio
from datalake import s3_utils
from datalake.config import Settings, load_settings

log = logging.getLogger(__name__)

GLUE_BATCH = 100   # máximo de particiones por llamada BatchCreate/BatchDelete


def s3_partitions(s3, bucket: str, prefix: str, key_name: str) -> set[str]:
    values = set()
    for key in s3_utils.list_keys(s3, bucket, prefix):
        if key.endswith(".parquet"):
            parts = pio.partition_values(key[len(prefix):])
            if key_name in parts:
                values.add(parts[key_name])
    return values


def glue_partitions(glue, database: str, table: str) -> set[str]:
    values = set()
    for page in glue.get_paginator("get_partitions").paginate(DatabaseName=database, TableName=table):
        values.update(p["Values"][0] for p in page["Partitions"])
    return values


def sync_table(glue, s3, bucket: str, database: str, table: str) -> dict:
    tbl = glue.get_table(DatabaseName=database, Name=table)["Table"]
    location = tbl["StorageDescriptor"]["Location"]           # s3://bucket/prefijo/
    prefix = location.split(f"s3://{bucket}/", 1)[1]
    key_name = tbl["PartitionKeys"][0]["Name"]

    in_s3 = s3_partitions(s3, bucket, prefix, key_name)
    in_glue = glue_partitions(glue, database, table)
    to_add, to_remove = sorted(in_s3 - in_glue), sorted(in_glue - in_s3)

    for i in range(0, len(to_add), GLUE_BATCH):
        inputs = []
        for value in to_add[i:i + GLUE_BATCH]:
            sd = copy.deepcopy(tbl["StorageDescriptor"])
            sd["Location"] = f"{location}{key_name}={value}/"
            inputs.append({"Values": [value], "StorageDescriptor": sd})
        resp = glue.batch_create_partition(DatabaseName=database, TableName=table,
                                           PartitionInputList=inputs)
        real_errors = [e for e in resp.get("Errors", [])
                       if e["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException"]
        if real_errors:
            raise RuntimeError(f"Glue no pudo crear particiones en {table}: {real_errors}")

    for i in range(0, len(to_remove), GLUE_BATCH):
        glue.batch_delete_partition(DatabaseName=database, TableName=table,
                                    PartitionsToDelete=[{"Values": [v]} for v in to_remove[i:i + GLUE_BATCH]])

    return {"table": table, "partitions_total": len(in_s3), "added": to_add, "removed": to_remove}


def sync_all(settings: Settings, run_id: str, s3=None, glue=None) -> list[dict]:
    s3 = s3 or s3_utils.s3_client(settings.region)
    glue = glue or boto3.client("glue", region_name=settings.region)
    tables = []
    for page in glue.get_paginator("get_tables").paginate(DatabaseName=settings.glue_database):
        tables.extend(t["Name"] for t in page["TableList"] if t.get("PartitionKeys"))
    results = [sync_table(glue, s3, settings.bucket, settings.glue_database, t) for t in sorted(tables)]
    for r in results:
        log.info("[run_id=%s][task=register_partitions] tabla=%s particiones=%s nuevas=%s eliminadas=%s",
                 run_id, r["table"], r["partitions_total"], r["added"], r["removed"])
    s3_utils.put_json(s3, settings.bucket, f"control/runs/run_id={run_id}/catalog.json",
                      {"run_id": run_id, "tables": results})
    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sync_all(load_settings(), sys.argv[1])
