"""Funciones puras de limpieza y normalización (sin AWS), fáciles de probar."""
from __future__ import annotations

import hashlib
import html
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

# Zona horaria asumida cuando una fecha llega SIN zona (El Tiempo publica en hora de Colombia).
DEFAULT_SOURCE_TZ = ZoneInfo("America/Bogota")
# Una fecha de publicación más allá de este margen en el futuro se considera inválida.
MAX_FUTURE_SKEW = timedelta(days=1)

# Estados de la fecha de publicación
DATE_OK = "OK"
DATE_TZ_ASSUMED = "TZ_ASSUMED"   # sin zona horaria: se asumió America/Bogota
DATE_MISSING = "MISSING"
DATE_INVALID = "INVALID"         # no se pudo interpretar
DATE_FUTURE = "FUTURE"           # interpretable pero imposible (muy en el futuro)

_WS_RE = re.compile(r"\s+")
_TRACKING_PARAMS = re.compile(r"^(utm_|fbclid$|gclid$|mc_cid$|mc_eid$)", re.I)


# ----------------------------------------------------------------------------- HTML
class _TextExtractor(HTMLParser):
    """Extrae solo el texto visible; descarta <script> y <style>."""

    _SKIP = {"script", "style"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP:
            self._skip_depth += 1
        elif tag in {"br", "p", "div", "li"}:
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in self._SKIP and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data):
        if not self._skip_depth:
            self.parts.append(data)


def clean_html(value: str | None) -> str:
    """Quita etiquetas HTML y entidades (también doblemente codificadas) y normaliza espacios."""
    if not value:
        return ""
    text = value
    # Algunos feeds codifican el HTML dos veces (&amp;lt;b&amp;gt;). Se decodifica hasta estabilizar.
    for _ in range(3):
        decoded = html.unescape(text)
        if decoded == text:
            break
        text = decoded
    parser = _TextExtractor()
    parser.feed(text)
    parser.close()
    text = "".join(parser.parts).replace("\xa0", " ")
    return _WS_RE.sub(" ", text).strip()


# ----------------------------------------------------------------------------- URLs
def is_valid_http_url(value: str | None) -> bool:
    if not value:
        return False
    parts = urlsplit(value.strip())
    return parts.scheme in ("http", "https") and bool(parts.netloc) and " " not in value.strip()


def normalize_url(value: str) -> str:
    """Forma canónica de una URL para usarla como identificador.

    - esquema y dominio en minúsculas, esquema http -> https
    - sin fragmento (#...), sin parámetros de seguimiento (utm_*, fbclid, ...)
    - sin barra final en la ruta
    """
    parts = urlsplit(value.strip())
    scheme = "https" if parts.scheme.lower() in ("http", "https") else parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path.rstrip("/") or "/"
    query = urlencode(
        [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
         if not _TRACKING_PARAMS.match(k)]
    )
    return urlunsplit((scheme, netloc, path, query, ""))


# ----------------------------------------------------------------------------- fechas
def parse_pub_date(
    raw: str | None, now: datetime | None = None
) -> tuple[datetime | None, str]:
    """Convierte la fecha de publicación a UTC. Devuelve (fecha_utc | None, estado).

    Acepta RFC 822 (formato clásico de RSS: 'Wed, 22 Oct 2026 06:11:08 -0500')
    e ISO 8601 ('2026-10-22T06:11:08-05:00', que es el que usa El Tiempo).
    """
    if raw is None or not raw.strip():
        return None, DATE_MISSING
    value = raw.strip()
    parsed: datetime | None = None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        parsed = None
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None, DATE_INVALID

    status = DATE_OK
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DEFAULT_SOURCE_TZ)
        status = DATE_TZ_ASSUMED
    utc = parsed.astimezone(timezone.utc)

    now = now or datetime.now(timezone.utc)
    if utc > now + MAX_FUTURE_SKEW:
        return None, DATE_FUTURE
    return utc, status


def normalize_term(value: str) -> str:
    """Clave de agrupación para entidades y frases: minúsculas, sin tildes, sin
    puntuación en los extremos y con espacios simples. 'Bogotá ' y 'BOGOTA' -> 'bogota'."""
    text = unicodedata.normalize("NFKD", value.casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _WS_RE.sub(" ", text).strip(" \t\n.,;:!?¡¿\"'«»()[]{}-–—")
    return text


def truncate_utf8(text: str, max_bytes: int) -> tuple[str, bool]:
    """Recorta un texto a max_bytes en UTF-8 sin partir un carácter. Devuelve (texto, recortado)."""
    data = text.encode("utf-8")
    if len(data) <= max_bytes:
        return text, False
    return data[:max_bytes].decode("utf-8", errors="ignore"), True


# ----------------------------------------------------------------------------- hashes
def sha256_text(*parts: str) -> str:
    """Hash estable de uno o varios textos (separados por un carácter que no aparece en noticias)."""
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
