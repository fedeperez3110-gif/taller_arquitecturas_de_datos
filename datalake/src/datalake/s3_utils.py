"""Funciones pequeñas sobre S3 usadas por todas las capas."""
from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

_NOT_FOUND = {"404", "NoSuchKey", "NotFound"}


def s3_client(region: str):
    return boto3.client("s3", region_name=region)


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in _NOT_FOUND:
            return False
        raise


def put_bytes(s3, bucket: str, key: str, data: bytes, content_type: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def put_json(s3, bucket: str, key: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    put_bytes(s3, bucket, key, body, "application/json")


def read_bytes(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def read_json(s3, bucket: str, key: str) -> dict[str, Any]:
    return json.loads(read_bytes(s3, bucket, key))


def delete_key(s3, bucket: str, key: str) -> None:
    s3.delete_object(Bucket=bucket, Key=key)


def list_prefixes(s3, bucket: str, prefix: str) -> list[str]:
    """Subcarpetas directas de un prefijo (p. ej. 'bronze/' -> ['bronze/source=a/', ...])."""
    out: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        out.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    return out


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
