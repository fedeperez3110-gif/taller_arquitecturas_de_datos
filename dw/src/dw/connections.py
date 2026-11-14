"""Conexiones a RDS MySQL, DocumentDB y Redshift Serverless, siempre cifradas (TLS).

Diferencias frente a la versión local:
  * MySQL: TLS con el certificado de AWS (global-bundle.pem).
  * DocumentDB: TLS obligatorio, replicaSet rs0, retryWrites=false (DocumentDB no admite
    escrituras reintentables) y lectura preferente de secundarias.
  * Redshift: redshift_connector con SSL; reemplaza el archivo DuckDB y su bloqueo de un solo escritor.
"""
from __future__ import annotations

import pymysql
import redshift_connector
from pymongo import MongoClient

from dw.settings import Settings, get_secret


def mysql_connect(settings: Settings, autocommit: bool = False, cursorclass=None):
    s = get_secret(settings.mysql_secret, settings.region)
    kwargs = dict(
        host=s["host"], port=int(s["port"]), user=s["username"], password=s["password"],
        database=s["database"], charset="utf8mb4", autocommit=autocommit,
        ssl={"ca": settings.ca_bundle}, connect_timeout=15,
    )
    if cursorclass is not None:
        kwargs["cursorclass"] = cursorclass
    return pymysql.connect(**kwargs)


def docdb_client(settings: Settings, **kwargs) -> MongoClient:
    s = get_secret(settings.docdb_secret, settings.region)
    return MongoClient(
        host=s["host"], port=int(s["port"]), username=s["username"], password=s["password"],
        tls=True, tlsCAFile=settings.ca_bundle, replicaSet="rs0",
        readPreference="secondaryPreferred", retryWrites=False,
        serverSelectionTimeoutMS=15000, **kwargs,
    )


def docdb_database(client: MongoClient, settings: Settings):
    return client[get_secret(settings.docdb_secret, settings.region)["database"]]


def redshift_connect(settings: Settings):
    s = get_secret(settings.redshift_secret, settings.region)
    conn = redshift_connector.connect(
        host=s["host"], port=int(s["port"]), user=s["username"], password=s["password"],
        database=s["database"], ssl=True, timeout=30,
    )
    conn.autocommit = False
    return conn


def redshift_query(settings: Settings, sql: str, params=None) -> list[tuple]:
    conn = redshift_connect(settings)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else []
        conn.commit()
        return [tuple(r) for r in rows]
    finally:
        conn.close()
