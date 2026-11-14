"""Carga de un lote a Redshift con COPY desde S3 (requisitos 3.3.1 y 3.3.2).

Todo el lote se carga en UNA transacción:
  DELETE FROM raw_data.<tabla> WHERE _batch_id = <lote>     (idempotencia: repetir no duplica)
  COPY raw_data.<tabla> FROM 's3://.../batch_id=<lote>/' IAM_ROLE '<rol>' FORMAT AS PARQUET
  verificación: filas del lote en Redshift == filas del manifiesto
Si algo falla se hace ROLLBACK y raw queda como estaba (simulate_failure lo demuestra).

raw_data ACUMULA los lotes (cada fila con _batch_id y _extracted_at); staging en dbt toma la
versión más reciente por llave de negocio.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from dw import batches, s3io
from dw.connections import redshift_connect
from dw.entities import ALL_ENTITIES, RAW_SCHEMA
from dw.settings import Settings

log = logging.getLogger(__name__)
BY_NAME = {e.name: e for e in ALL_ENTITIES}


def load_batch(settings: Settings, batch_id: str, s3=None, connection_factory=None,
               simulate_failure: bool = False) -> dict:
    batches.validate_batch_id(batch_id)
    s3 = s3 or s3io.client(settings.region)
    manifests = []
    for source in ("mysql", "documentdb"):
        key = batches.control_key(batch_id, f"extract_{source}")
        if not s3io.exists(s3, settings.bucket, key):
            raise RuntimeError(f"[batch_id={batch_id}] falta el manifiesto de extracción de {source}")
        manifests.append(s3io.read_json(s3, settings.bucket, key))

    conn = (connection_factory or redshift_connect)(settings)
    results = []
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        for manifest in manifests:
            for item in manifest["entities"]:
                entity = BY_NAME[item["entity"]]
                cur.execute(entity.redshift_ddl())
                cur.execute(f"DELETE FROM {entity.raw_table} WHERE _batch_id = %s", (batch_id,))
                if item["rows"]:
                    cur.execute(
                        f"COPY {entity.raw_table} FROM 's3://{settings.bucket}/{entity.s3_prefix(batch_id)}' "
                        f"IAM_ROLE '{settings.redshift_iam_role}' FORMAT AS PARQUET"
                    )
                cur.execute(f"SELECT COUNT(*) FROM {entity.raw_table} WHERE _batch_id = %s", (batch_id,))
                loaded = int(cur.fetchone()[0])
                if simulate_failure:
                    # Fallo controlado DESPUÉS de un COPY real: demuestra que el ROLLBACK deja raw_data
                    # sin filas del lote y que la marca de avance no se mueve.
                    raise RuntimeError(f"[batch_id={batch_id}][entity={entity.name}] FALLO SIMULADO "
                                       "después del COPY (simulate_failure=true)")
                if loaded != item["rows"]:
                    raise RuntimeError(f"[batch_id={batch_id}][entity={entity.name}] COPY cargó {loaded} "
                                       f"filas y el manifiesto dice {item['rows']}")
                results.append({"entity": entity.name, "raw_table": entity.raw_table,
                                "rows_in_manifest": item["rows"], "rows_loaded": loaded})
                log.info("[batch_id=%s][entity=%s] COPY ok: %s filas", batch_id, entity.name, loaded)
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("[batch_id=%s] carga fallida: ROLLBACK, raw_data sin cambios", batch_id)
        raise
    finally:
        conn.close()

    summary = {"batch_id": batch_id, "loaded_at_utc": datetime.now(timezone.utc).isoformat(),
               "total_rows": sum(r["rows_loaded"] for r in results), "entities": results}
    s3io.put_json(s3, settings.bucket, batches.control_key(batch_id, "load"), summary)
    return summary
