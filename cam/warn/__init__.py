from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .state_urls import StateConfig

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = 60


@dataclass
class WarnRecord:
    """A single WARN notice row after normalisation."""

    state_code: str
    company: str
    notice_date: date | None
    employees_affected: int | None
    city: str
    county: str
    layoff_type: str  # "Layoff" | "Closure" | other state-specific values
    raw: dict[str, Any] = field(default_factory=dict)


FIELDNAMES = [
    "key",
    "state",
    "employer",
    "notice_date",
    "employees",
    "city",
    "county",
    "notice_type",
    "source_url",
    "first_seen",
]


def _is_retriable(exc: BaseException) -> bool:
    """Return True for transient network errors and HTTP 429/5xx errors."""

    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (
        429,  # rate-limited
        500,  # internal server error (transient)
        502,  # bad gateway
        503,  # service unavailable
        504,  # gateway timeout
    ):
        return True
    return False


def fetch(url: str, *, client: httpx.Client | None = None) -> bytes:
    """GET *url* with retry; returns raw response bytes."""

    headers = {"User-Agent": USER_AGENT}

    @retry(
        retry=retry_if_exception(_is_retriable),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _request() -> bytes:
        if client is not None:
            resp = client.get(
                url,
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
                headers=headers,
            )
        else:
            resp = httpx.get(
                url,
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
                headers=headers,
            )
        resp.raise_for_status()
        return resp.content

    return _request()


def _parse_date(value: str | None, fmt: str = "%m/%d/%Y") -> date | None:
    """Parse a date string with the given format; return None on failure."""
    raw = (value or "").strip()
    if not raw:
        return None
    for f in (fmt, "%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw, f).date()
        except ValueError:
            continue
    logger.warning("WARN: could not parse date %r", value)
    return None


def _parse_employees(value: str | None) -> int | None:
    """Parse employee-count strings like '350' or '1,200'."""
    raw = (value or "").strip().replace(",", "")
    try:
        n = int(raw)
        return n if n > 0 else None
    except ValueError:
        return None


def _clean_name(raw: str | None) -> str:
    """Strip trailing location suffixes like '- OAKLAND' from establishment names."""
    cleaned = (raw or "").strip()
    cleaned = re.sub(r"\s+-\s+[A-Z0-9 ]+$", "", cleaned)
    return cleaned.strip()


def _parse_csv(content: bytes, cfg: StateConfig) -> list[WarnRecord]:
    """Parse a CSV-format WARN file according to *cfg* column mappings."""
    import csv

    cols = cfg.columns
    records: list[WarnRecord] = []
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        raw_company = row.get(cols.get("company", ""), "")
        records.append(
            WarnRecord(
                state_code=cfg.state_code,
                company=_clean_name(raw_company),
                notice_date=_parse_date(row.get(cols.get("date", ""), ""), cfg.date_fmt),
                employees_affected=_parse_employees(row.get(cols.get("employees", ""), "")),
                city=(row.get(cols.get("city", ""), "") or "").strip(),
                county=(row.get(cols.get("county", ""), "") or "").strip(),
                layoff_type=(row.get(cols.get("layoff_type", ""), "") or "").strip(),
                raw=dict(row),
            )
        )
    return records


def _parse_html(content: bytes, cfg: StateConfig) -> list[WarnRecord]:
    """Parse an HTML table WARN page according to *cfg* column mappings.

    Locates the first ``<table>`` tag (or the one with id matching
    *cfg.html_table_id* if set) and extracts rows.  Falls back gracefully
    if BeautifulSoup is unavailable.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:  # pragma: no cover
        logger.error("beautifulsoup4 not installed; cannot parse HTML WARN pages")
        return []

    cols = cfg.columns
    soup = BeautifulSoup(content.decode("utf-8", errors="replace"), "html.parser")

    # Find the right table
    table = None
    if cfg.html_table_id:
        table = soup.find("table", {"id": cfg.html_table_id})
    if table is None:
        table = soup.find("table")
    if table is None:
        logger.warning("WARN[%s]: no <table> found in HTML page", cfg.state_code)
        return []

    # Extract headers from the first <tr> with <th> cells
    headers: list[str] = []
    for tr in table.find_all("tr"):
        ths = tr.find_all("th")
        if ths:
            headers = [th.get_text(strip=True) for th in ths]
            break

    if not headers:
        logger.warning("WARN[%s]: no <th> headers found in table", cfg.state_code)
        return []

    records: list[WarnRecord] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds or len(tds) != len(headers):
            continue
        row = {headers[i]: tds[i].get_text(strip=True) for i in range(len(headers))}
        raw_company = row.get(cols.get("company", ""), "")
        records.append(
            WarnRecord(
                state_code=cfg.state_code,
                company=_clean_name(raw_company),
                notice_date=_parse_date(row.get(cols.get("date", ""), ""), cfg.date_fmt),
                employees_affected=_parse_employees(row.get(cols.get("employees", ""), "")),
                city=(row.get(cols.get("city", ""), "") or "").strip(),
                county=(row.get(cols.get("county", ""), "") or "").strip(),
                layoff_type=(row.get(cols.get("layoff_type", ""), "") or "").strip(),
                raw=row,
            )
        )
    return records


def parse(content: bytes, cfg: StateConfig) -> list[WarnRecord]:
    """Dispatch raw *content* to the parser for *cfg.format*."""
    if cfg.format == "csv":
        return _parse_csv(content, cfg)
    if cfg.format == "html":
        return _parse_html(content, cfg)
    if cfg.format == "xlsx":
        from ._xlsx import parse_ca_xlsx

        return parse_ca_xlsx(content)
    if cfg.format == "mi_json":
        from ._mi import parse_mi

        return parse_mi(content)
    raise ValueError(f"Unsupported format: {cfg.format!r}")


def _collapse_whitespace(text: str) -> str:
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def record_key(record: WarnRecord) -> str:
    """Return a stable dedup key for a WARN record."""

    company = _collapse_whitespace(record.company).lower()
    city = _collapse_whitespace(record.city).lower()
    if record.notice_date is not None:
        date_part = record.notice_date.isoformat()
    else:
        raw_json = json.dumps(record.raw, sort_keys=True, default=str)
        date_part = "no-date:" + hashlib.sha256(raw_json.encode()).hexdigest()[:16]
    return f"{record.state_code}::{company}::{date_part}::{city}"


def record_to_row(
    record: WarnRecord,
    *,
    source_url: str,
    first_seen: date,
) -> dict[str, str]:
    """Convert a WARN record into the CSV-like row schema."""

    def keep_case(value: str) -> str:
        return _collapse_whitespace(value)

    employees = "" if record.employees_affected is None else str(int(record.employees_affected))
    notice_date = "" if record.notice_date is None else record.notice_date.isoformat()
    return {
        "key": record_key(record),
        "state": record.state_code,
        "employer": keep_case(record.company),
        "notice_date": notice_date,
        "employees": employees,
        "city": keep_case(record.city),
        "county": keep_case(record.county),
        "notice_type": keep_case(record.layoff_type),
        "source_url": source_url,
        "first_seen": first_seen.isoformat(),
    }


__all__ = [
    "WarnRecord",
    "USER_AGENT",
    "HTTP_TIMEOUT",
    "FIELDNAMES",
    "fetch",
    "parse",
    "record_key",
    "record_to_row",
]
