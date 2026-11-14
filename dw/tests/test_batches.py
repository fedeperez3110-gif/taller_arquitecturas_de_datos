from datetime import datetime, timezone

import pytest

from dw import batches


def test_batch_id_is_real_utc_time():
    assert batches.new_batch_id(datetime(2026, 11, 13, 21, 5, 9, tzinfo=timezone.utc)) == "20261113T210509Z"


@pytest.mark.parametrize("bad", ["", "2026-11-13", "20261113T2105Z", "../x", None])
def test_invalid_batch_ids_are_rejected(bad):
    with pytest.raises(ValueError):
        batches.validate_batch_id(bad)


def test_retries_of_the_same_airflow_run_reuse_the_batch(s3, settings):
    first = batches.resolve_batch_id(s3, settings.bucket, "manual__2026-11-13T21:00:00+00:00",
                                     now=datetime(2026, 11, 13, 21, 0, 1, tzinfo=timezone.utc))
    again = batches.resolve_batch_id(s3, settings.bucket, "manual__2026-11-13T21:00:00+00:00",
                                     now=datetime(2026, 11, 13, 22, 0, 0, tzinfo=timezone.utc))
    assert first == again == "20261113T210001Z"


def test_watermark_comes_from_last_complete_batch_only(s3, settings):
    from dw import s3io
    for bid, status, wm in (("20261101T000000Z", "COMPLETE", 10), ("20261102T000000Z", "FAILED", 20)):
        s3io.put_json(s3, settings.bucket, batches.control_key(bid, "extract_mysql"),
                      {"entities": [{"entity": "orders", "watermark_to": wm}]})
        batches.record_status(s3, settings.bucket, bid, status, {})
    ref, wms = batches.previous_watermarks(s3, settings.bucket, "mysql", "20261103T000000Z")
    assert ref == "20261101T000000Z" and wms == {"orders": 10}
    # un lote nunca toma como referencia a sí mismo ni a uno posterior
    assert batches.previous_watermarks(s3, settings.bucket, "mysql", "20261101T000000Z") == (None, {})
