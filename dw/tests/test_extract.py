from datetime import datetime, timezone
from decimal import Decimal
import io

import pyarrow.parquet as pq

from conftest import FakeMongo, FakeMySQL
from dw import batches, s3io
from dw.extract import extract_source, flatten_document

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def mysql_tables(n_orders=2):
    ts = datetime(2025, 1, 1)
    return {
        "customers": [{"customer_id": 1, "name": "Ana", "email": "a@x.co", "phone": "1", "city": "Bogotá",
                       "department": "Bogotá, D.C.", "registration_date": ts.date(), "updated_at": ts}],
        "products": [{"product_id": 1, "name": "Lámpara", "category": "Hogar", "price": Decimal("10.50"),
                      "description": "d", "is_active": 1, "updated_at": ts}],
        "orders": [{"order_id": i, "customer_id": 1, "order_date": ts, "status": "pagado", "sales_channel": "web",
                    "payment_method": "PSE", "total": Decimal("10.50")} for i in range(1, n_orders + 1)],
        "order_items": [{"order_item_id": i, "order_id": i, "product_id": 1, "quantity": 1,
                         "unit_price": Decimal("10.50"), "subtotal": Decimal("10.50")} for i in range(1, n_orders + 1)],
    }


def read_parquet(s3, bucket, key):
    return pq.read_table(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))


def test_full_extraction_writes_parquet_and_manifest(s3, settings):
    fake = FakeMySQL(mysql_tables())
    m = extract_source(settings, "mysql", "20261113T120000Z", s3=s3, connection_factory=lambda _: fake, now=NOW)
    assert m["action"] == "EXTRACTED" and m["mode"] == "full"
    rows = {e["entity"]: e["rows"] for e in m["entities"]}
    assert rows == {"customers": 1, "products": 1, "orders": 2, "order_items": 2}
    table = read_parquet(s3, settings.bucket, "raw/mysql/orders/batch_id=20261113T120000Z/part-00000.parquet")
    assert table.column("_batch_id").to_pylist() == ["20261113T120000Z"] * 2
    assert table.column_names[-3:] == ["_batch_id", "_extracted_at", "_source"]
    products = read_parquet(s3, settings.bucket, "raw/mysql/products/batch_id=20261113T120000Z/part-00000.parquet")
    assert products.column("is_active").to_pylist() == [True]


def test_repeating_a_batch_does_not_query_the_source(s3, settings):
    extract_source(settings, "mysql", "20261113T120000Z", s3=s3,
                   connection_factory=lambda _: FakeMySQL(mysql_tables()), now=NOW)
    second = FakeMySQL(mysql_tables())
    m = extract_source(settings, "mysql", "20261113T120000Z", s3=s3, connection_factory=lambda _: second)
    assert m["action"] == "REUSED" and second.queries == []


def test_incremental_uses_watermark_of_last_complete_batch(s3, settings):
    first = "20261113T120000Z"
    extract_source(settings, "mysql", first, s3=s3, connection_factory=lambda _: FakeMySQL(mysql_tables(2)), now=NOW)
    batches.record_status(s3, settings.bucket, first, batches.COMPLETE, {})
    fake = FakeMySQL(mysql_tables(5))
    m = extract_source(settings, "mysql", "20261113T130000Z", s3=s3, connection_factory=lambda _: fake, now=NOW)
    entities = {e["entity"]: e for e in m["entities"]}
    assert m["mode"] == "incremental" and m["watermarks_from_batch"] == first
    assert entities["orders"]["rows"] == 3 and entities["orders"]["watermark_from"] == 2
    assert entities["orders"]["watermark_to"] == 5
    # updated_at >= marca: no se pierden cambios del mismo segundo
    assert entities["customers"]["rows"] == 1
    assert any("order_id > %s" in sql for sql, _ in fake.queries)


def test_failed_batch_does_not_advance_the_watermark(s3, settings):
    extract_source(settings, "mysql", "20261113T120000Z", s3=s3,
                   connection_factory=lambda _: FakeMySQL(mysql_tables(2)), now=NOW)
    batches.record_status(s3, settings.bucket, "20261113T120000Z", batches.COMPLETE, {})
    extract_source(settings, "mysql", "20261113T130000Z", s3=s3,
                   connection_factory=lambda _: FakeMySQL(mysql_tables(4)), now=NOW)
    batches.record_status(s3, settings.bucket, "20261113T130000Z", batches.FAILED, {})
    m = extract_source(settings, "mysql", "20261113T140000Z", s3=s3,
                       connection_factory=lambda _: FakeMySQL(mysql_tables(4)), now=NOW)
    orders = next(e for e in m["entities"] if e["entity"] == "orders")
    assert m["watermarks_from_batch"] == "20261113T120000Z" and orders["rows"] == 2


def test_full_mode_ignores_watermarks(s3, settings):
    extract_source(settings, "mysql", "20261113T120000Z", s3=s3,
                   connection_factory=lambda _: FakeMySQL(mysql_tables(2)), now=NOW)
    batches.record_status(s3, settings.bucket, "20261113T120000Z", batches.COMPLETE, {})
    m = extract_source(settings, "mysql", "20261113T130000Z", mode="full", s3=s3,
                       connection_factory=lambda _: FakeMySQL(mysql_tables(3)), now=NOW)
    assert m["mode"] == "full" and next(e for e in m["entities"] if e["entity"] == "orders")["rows"] == 3


def test_documentdb_extraction_flattens_and_keeps_full_document(s3, settings):
    utc = datetime(2025, 3, 1, 15, 0, tzinfo=timezone.utc)
    reviews = [{"_id": i, "review_id": i, "rating": 4, "reviewed_at": utc,
                "customer": {"customer_id": 1}, "product": {"product_id": 2},
                "verification": {"purchase_confirmed": True}} for i in (1, 2)]
    events = [{"_id": 7, "event_id": 7, "event_type": "busqueda", "occurred_at": utc,
               "customer": {"customer_id": 1}, "product": {"product_id": 2}, "device": {"sales_channel": "web"}}]
    fake = FakeMongo({"reviews": reviews, "navigation_events": events})
    m = extract_source(settings, "documentdb", "20261113T120000Z", s3=s3, connection_factory=lambda _: fake, now=NOW)
    assert {e["entity"]: e["rows"] for e in m["entities"]} == {"reviews": 2, "navigation_events": 1}
    t = read_parquet(s3, settings.bucket, "raw/documentdb/reviews/batch_id=20261113T120000Z/part-00000.parquet")
    assert t.column("customer_id").to_pylist() == [1, 1]
    assert '"purchase_confirmed": true' in t.column("document_json").to_pylist()[0]
    assert t.column("reviewed_at").to_pylist()[0] == datetime(2025, 3, 1, 15, 0)   # UTC sin zona


def test_flatten_document_tolerates_missing_nested_fields():
    row = flatten_document("navigation_events", {"event_id": 1, "event_type": "x"})
    assert row["customer_id"] is None and row["sales_channel"] is None and row["document_json"]
