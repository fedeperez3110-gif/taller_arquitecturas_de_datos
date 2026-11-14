"""Conciliación del indicador de ventas entre la FUENTE (RDS MySQL) y el modelo analítico.

Tres igualdades que deben cumplirse en cada lote:
  1. Fuente MySQL (todas las líneas)          == raw_data en Redshift (versión vigente por llave)
     -> la extracción incremental y el COPY no perdieron ni duplicaron registros.
  2. Líneas válidas (intermediate)             == marts.fct_sales (conteo y suma de subtotal)
     -> el modelo analítico contiene exactamente los datos válidos.
  3. Líneas de la fuente - líneas en cuarentena == marts.fct_sales
     -> cada línea está en el hecho o en cuarentena (con motivo y lote de origen), nunca en ambos.
"""
from __future__ import annotations

import logging
from decimal import Decimal

from dw import batches, s3io
from dw.connections import mysql_connect, redshift_query
from dw.settings import Settings

log = logging.getLogger(__name__)

RAW_LATEST = """
select count(*), coalesce(sum(subtotal), 0) from (
  select subtotal, row_number() over (partition by order_item_id
                                      order by _extracted_at desc, _batch_id desc) as rn
  from raw_data.mysql_order_items) t
where rn = 1
"""
VALID = """
select count(*), coalesce(sum(i.subtotal), 0)
from intermediate.int_mysql__order_items__valid i
join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id
"""
FACT = "select count(*), coalesce(sum(subtotal), 0) from marts.fct_sales"
QUARANTINE = "select count(*) from quarantine.quarantine_mysql__order_items"


def _pair(row) -> tuple[int, Decimal]:
    return int(row[0]), Decimal(str(row[1])).quantize(Decimal("0.01"))


def reconcile(settings: Settings, batch_id: str) -> dict:
    conn = mysql_connect(settings)
    try:
        with conn.cursor() as cur:
            cur.execute("select count(*), coalesce(sum(subtotal), 0) from order_items")
            source = _pair(cur.fetchone())
    finally:
        conn.close()
    raw = _pair(redshift_query(settings, RAW_LATEST)[0])
    valid = _pair(redshift_query(settings, VALID)[0])
    fact = _pair(redshift_query(settings, FACT)[0])
    quarantined = int(redshift_query(settings, QUARANTINE)[0][0])

    checks = {
        "source_equals_raw": source == raw,
        "valid_equals_fact": valid == fact,
        # Una línea cuyo pedido se rechaza también se rechaza (orphan_order), así que
        # fuente = hecho + cuarentena.
        "source_minus_quarantine_equals_fact": source[0] - quarantined == fact[0],
    }
    result = {
        "batch_id": batch_id,
        "indicator": "ventas (count y sum(subtotal) de líneas de pedido)",
        "source_mysql": {"rows": source[0], "subtotal": str(source[1])},
        "raw_latest_redshift": {"rows": raw[0], "subtotal": str(raw[1])},
        "valid_intermediate": {"rows": valid[0], "subtotal": str(valid[1])},
        "fct_sales": {"rows": fact[0], "subtotal": str(fact[1])},
        "quarantined_order_items": quarantined,
        "checks": checks, "passed": all(checks.values()),
    }
    s3io.put_json(s3io.client(settings.region), settings.bucket, batches.control_key(batch_id, "reconciliation"), result)
    log.info("[batch_id=%s][task=reconcile] fuente=%s raw=%s válidas=%s hecho=%s cuarentena=%s checks=%s",
             batch_id, source, raw, valid, fact, quarantined, checks)
    if not result["passed"]:
        raise RuntimeError(f"[batch_id={batch_id}] conciliación fallida: {checks}")
    return result
