"""Definición única de las 6 entidades extraídas: columnas, tipos Parquet/Redshift y marca de avance.

Una sola fuente de verdad para el esquema evita que el archivo Parquet y la tabla de Redshift se
desalineen (COPY ... FORMAT AS PARQUET asigna las columnas por posición).
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

RAW_SCHEMA = "raw_data"   # "raw" es palabra reservada en Redshift

# (nombre, tipo pyarrow, tipo Redshift)
STR = (pa.string(), "VARCHAR(512)")
LONGSTR = (pa.string(), "VARCHAR(2048)")
JSON = (pa.string(), "VARCHAR(65535)")
BIGINT = (pa.int64(), "BIGINT")
INT = (pa.int32(), "INTEGER")
DATE = (pa.date32(), "DATE")
TS = (pa.timestamp("us"), "TIMESTAMP")          # sin zona: MySQL en hora de negocio, Mongo en UTC
BOOL = (pa.bool_(), "BOOLEAN")


def DEC(p: int, s: int):
    return (pa.decimal128(p, s), f"DECIMAL({p},{s})")


METADATA = [("_batch_id", STR), ("_extracted_at", TS), ("_source", STR)]


@dataclass(frozen=True)
class Entity:
    source: str              # mysql | documentdb
    name: str                # customers, orders, reviews...
    columns: list            # [(nombre, (tipo_arrow, tipo_redshift))]
    key: str                 # llave de negocio
    watermark: str           # columna de la marca de avance
    watermark_kind: str      # "timestamp" (>=, cambios) | "id" (>, solo inserciones)

    @property
    def raw_table(self) -> str:
        prefix = "mysql" if self.source == "mysql" else "mongo"
        return f"{RAW_SCHEMA}.{prefix}_{self.name}"

    @property
    def all_columns(self) -> list:
        return self.columns + METADATA

    def arrow_schema(self) -> pa.Schema:
        return pa.schema([(n, t[0]) for n, t in self.all_columns])

    def redshift_ddl(self) -> str:
        cols = ",\n    ".join(f"{n} {t[1]}" for n, t in self.all_columns)
        return f"CREATE TABLE IF NOT EXISTS {self.raw_table} (\n    {cols}\n)"

    def s3_prefix(self, batch_id: str) -> str:
        return f"raw/{self.source}/{self.name}/batch_id={batch_id}/"


MYSQL_ENTITIES = [
    Entity("mysql", "customers", [
        ("customer_id", BIGINT), ("name", STR), ("email", STR), ("phone", STR), ("city", STR),
        ("department", STR), ("registration_date", DATE), ("updated_at", TS),
    ], key="customer_id", watermark="updated_at", watermark_kind="timestamp"),
    Entity("mysql", "products", [
        ("product_id", BIGINT), ("name", STR), ("category", STR), ("price", DEC(12, 2)),
        ("description", LONGSTR), ("is_active", BOOL), ("updated_at", TS),
    ], key="product_id", watermark="updated_at", watermark_kind="timestamp"),
    Entity("mysql", "orders", [
        ("order_id", BIGINT), ("customer_id", BIGINT), ("order_date", TS), ("status", STR),
        ("sales_channel", STR), ("payment_method", STR), ("total", DEC(14, 2)),
    ], key="order_id", watermark="order_id", watermark_kind="id"),
    Entity("mysql", "order_items", [
        ("order_item_id", BIGINT), ("order_id", BIGINT), ("product_id", BIGINT), ("quantity", INT),
        ("unit_price", DEC(12, 2)), ("subtotal", DEC(14, 2)),
    ], key="order_item_id", watermark="order_item_id", watermark_kind="id"),
]

MONGO_ENTITIES = [
    Entity("documentdb", "reviews", [
        ("review_id", BIGINT), ("customer_id", BIGINT), ("product_id", BIGINT), ("rating", INT),
        ("reviewed_at", TS), ("document_json", JSON),
    ], key="review_id", watermark="review_id", watermark_kind="id"),
    Entity("documentdb", "navigation_events", [
        ("event_id", BIGINT), ("event_type", STR), ("occurred_at", TS), ("customer_id", BIGINT),
        ("product_id", BIGINT), ("sales_channel", STR), ("document_json", JSON),
    ], key="event_id", watermark="event_id", watermark_kind="id"),
]

ALL_ENTITIES = MYSQL_ENTITIES + MONGO_ENTITIES
BY_SOURCE = {"mysql": MYSQL_ENTITIES, "documentdb": MONGO_ENTITIES}
