"""Configuración y credenciales.

Los valores no secretos vienen de variables de entorno (dw/.env en la EC2). Las credenciales de
MySQL, DocumentDB y Redshift se leen de AWS Secrets Manager con el rol IAM de la EC2: nunca se
escriben en archivos.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache

import boto3


@dataclass(frozen=True)
class Settings:
    bucket: str
    region: str
    mysql_secret: str
    docdb_secret: str
    redshift_secret: str
    redshift_iam_role: str
    ca_bundle: str
    dbt_project_dir: str
    dbt_executable: str


def load_settings() -> Settings:
    def need(name: str) -> str:
        value = os.environ.get(name, "").strip()
        if not value:
            raise RuntimeError(f"Falta la variable de entorno {name} (ver dw/.env.example)")
        return value

    return Settings(
        bucket=need("DW_BUCKET"),
        region=os.environ.get("DW_REGION", "us-east-2"),
        mysql_secret=need("DW_MYSQL_SECRET"),
        docdb_secret=need("DW_DOCDB_SECRET"),
        redshift_secret=need("DW_REDSHIFT_SECRET"),
        redshift_iam_role=need("DW_REDSHIFT_IAM_ROLE"),
        ca_bundle=os.environ.get("DW_CA_BUNDLE", "/opt/certs/global-bundle.pem"),
        dbt_project_dir=os.environ.get("DW_DBT_PROJECT_DIR", "/opt/airflow/dw/dbt"),
        dbt_executable=os.environ.get("DW_DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt"),
    )


@lru_cache(maxsize=8)
def get_secret(secret_id: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    return json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
