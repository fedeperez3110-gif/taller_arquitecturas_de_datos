import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dw.settings import Settings  # noqa: E402

BUCKET = "ecommerce-dw-test"


@pytest.fixture
def settings():
    return Settings(bucket=BUCKET, region="us-east-2", mysql_secret="m", docdb_secret="d",
                    redshift_secret="r", redshift_iam_role="arn:aws:iam::1:role/copy",
                    ca_bundle="/tmp/ca.pem", dbt_project_dir="/tmp/dbt", dbt_executable="dbt")


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-2")
        client.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        yield client


# ------------------------------------------------------------------ fuentes falsas
class FakeCursor:
    def __init__(self, tables, log):
        self.tables, self.log, self.rows = tables, log, []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.log.append((sql, params))
        table = sql.split(" FROM ")[1].split()[0]
        rows = self.tables.get(table, [])
        if " WHERE " in sql:
            col, op = sql.split(" WHERE ")[1].split()[:2]
            value = params[0]
            rows = [r for r in rows if (r[col] >= value if op == ">=" else r[col] > value)]
        self.rows = rows

    def fetchall(self):
        return list(self.rows)


class FakeMySQL:
    def __init__(self, tables):
        self.tables, self.queries, self.closed = tables, [], False

    def cursor(self, *a):
        return FakeCursor(self.tables, self.queries)

    def close(self):
        self.closed = True


class FakeFind:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, key, direction):
        self.docs = sorted(self.docs, key=lambda d: d[key])
        return self

    def batch_size(self, n):
        return iter(self.docs)


class FakeCollection:
    def __init__(self, docs, log):
        self.docs, self.log = docs, log

    def find(self, query):
        self.log.append(query)
        docs = self.docs
        for field, cond in query.items():
            docs = [d for d in docs if d[field] > cond["$gt"]]
        return FakeFind(docs)


class FakeMongo:
    def __init__(self, collections):
        self.queries = []
        self.db = {n: FakeCollection(d, self.queries) for n, d in collections.items()}

    def __getitem__(self, name):
        return self.db

    def close(self):
        pass
