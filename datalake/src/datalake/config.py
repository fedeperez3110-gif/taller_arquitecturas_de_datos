"""Carga y valida la configuración del pipeline.

La configuración de negocio (fuentes RSS) vive en config/sources.yaml (versionado en git).
Los parámetros de despliegue (bucket, región) vienen de variables de entorno (.env),
para que el repositorio no tenga valores propios de una cuenta AWS.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

import yaml

SOURCE_ID_RE = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True)
class Source:
    source_id: str
    media: str
    section: str
    url: str


@dataclass(frozen=True)
class Settings:
    bucket: str
    region: str
    glue_database: str
    athena_workgroup: str
    http_timeout: int
    user_agent: str
    sources: list[Source] = field(default_factory=list)

    def source(self, source_id: str) -> Source:
        for s in self.sources:
            if s.source_id == source_id:
                return s
        raise KeyError(f"Fuente desconocida: {source_id}")


class ConfigError(ValueError):
    pass


def load_settings(path: str | None = None) -> Settings:
    path = path or os.environ.get("DATALAKE_CONFIG", "config/sources.yaml")
    with open(path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}

    bucket = os.environ.get("DATALAKE_BUCKET", "").strip()
    if not bucket:
        raise ConfigError("Falta la variable de entorno DATALAKE_BUCKET")

    media = cfg.get("media")
    if not media:
        raise ConfigError("sources.yaml debe definir 'media'")

    sources = []
    for raw in cfg.get("sources", []):
        src = Source(media=media, **raw)
        if not SOURCE_ID_RE.match(src.source_id):
            raise ConfigError(f"source_id inválido (solo a-z, 0-9, _): {src.source_id}")
        parsed = urlparse(src.url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ConfigError(f"URL inválida para {src.source_id}: {src.url}")
        sources.append(src)

    if not sources:
        raise ConfigError("sources.yaml no tiene fuentes")
    ids = [s.source_id for s in sources]
    if len(set(ids)) != len(ids):
        raise ConfigError(f"source_id repetidos: {ids}")

    http = cfg.get("http", {})
    return Settings(
        bucket=bucket,
        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        glue_database=os.environ.get("GLUE_DATABASE", "rss_datalake"),
        athena_workgroup=os.environ.get("ATHENA_WORKGROUP", "rss-datalake"),
        http_timeout=int(http.get("timeout_seconds", 20)),
        user_agent=http.get("user_agent", "EIA-DataLake-Lab/1.0"),
        sources=sources,
    )
