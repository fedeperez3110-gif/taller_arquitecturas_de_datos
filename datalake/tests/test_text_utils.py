from datetime import datetime, timezone

import pytest

from datalake.text_utils import (
    DATE_FUTURE, DATE_INVALID, DATE_MISSING, DATE_OK, DATE_TZ_ASSUMED,
    clean_html, is_valid_http_url, normalize_url, parse_pub_date, sha256_text,
)

NOW = datetime(2026, 11, 12, 2, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("raw,expected", [
    ("Petro habl&#039;o en <b>Medellín</b>", "Petro habl'o en Medellín"),
    ("&lt;p&gt;Hola&lt;/p&gt;&lt;p&gt;mundo&lt;/p&gt;", "Hola mundo"),            # HTML escapado
    ("&amp;lt;b&amp;gt;doble&amp;lt;/b&amp;gt;", "doble"),                          # doble escape
    ('<img src="x.jpg"/>Texto<script>alert(1)</script>  final', "Texto final"),
    ("  muchos\n\n espacios\xa0aquí ", "muchos espacios aquí"),
    (None, ""),
    ("", ""),
])
def test_clean_html(raw, expected):
    assert clean_html(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("https://www.eltiempo.com/a/b/", "https://www.eltiempo.com/a/b"),
    ("HTTP://WWW.ElTiempo.com/a?utm_source=rss&id=3#top", "https://www.eltiempo.com/a?id=3"),
    ("https://www.eltiempo.com/a?fbclid=xyz", "https://www.eltiempo.com/a"),
])
def test_normalize_url(raw, expected):
    assert normalize_url(raw) == expected


def test_same_news_different_tracking_gives_same_id():
    a = normalize_url("https://www.eltiempo.com/x/nota-123?utm_medium=rss")
    b = normalize_url("https://www.eltiempo.com/x/nota-123/")
    assert a == b


@pytest.mark.parametrize("raw,ok", [
    ("https://www.eltiempo.com/x", True),
    ("ftp://x.com/a", False),
    ("www.eltiempo.com/x", False),
    ("https://", False),
    ("", False),
    (None, False),
])
def test_is_valid_http_url(raw, ok):
    assert is_valid_http_url(raw) is ok


@pytest.mark.parametrize("raw,expected_utc,status", [
    ("2026-11-11T06:11:08-05:00", datetime(2026, 11, 11, 11, 11, 8, tzinfo=timezone.utc), DATE_OK),
    ("Wed, 11 Nov 2026 06:11:08 -0500", datetime(2026, 11, 11, 11, 11, 8, tzinfo=timezone.utc), DATE_OK),
    ("2026-11-11T11:11:08Z", datetime(2026, 11, 11, 11, 11, 8, tzinfo=timezone.utc), DATE_OK),
    ("2026-11-11T06:11:08", datetime(2026, 11, 11, 11, 11, 8, tzinfo=timezone.utc), DATE_TZ_ASSUMED),
    ("", None, DATE_MISSING),
    (None, None, DATE_MISSING),
    ("ayer en la tarde", None, DATE_INVALID),
    ("2030-01-01T00:00:00Z", None, DATE_FUTURE),
])
def test_parse_pub_date(raw, expected_utc, status):
    utc, st = parse_pub_date(raw, now=NOW)
    assert st == status
    assert utc == expected_utc


def test_sha256_text_is_stable_and_separates_fields():
    assert sha256_text("a", "b") == sha256_text("a", "b")
    assert sha256_text("ab", "") != sha256_text("a", "b")
