from __future__ import annotations

import csv
import io
import re
import tempfile
import zipfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

BULK_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
FIELDNAMES = ["company", "month", "product", "complaints"]
WINDOW_MONTHS = 24
MIN_COMPLAINTS = 1000


def _is_retriable_download_exc(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    return False


def stream_to_tempfile(
    url: str,
    *,
    client: httpx.Client | None = None,
    timeout: float = 600,
    chunk_size: int = 1 << 20,
) -> Path:
    """Stream *url* into a temporary file and return its path."""

    import logging

    logger = logging.getLogger(__name__)
    tmp: tempfile.NamedTemporaryFile
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
        tmp_path = Path(tmp.name)

        def _write_chunked_response(resp: httpx.Response) -> None:
            tmp.seek(0)
            tmp.truncate(0)
            for chunk in resp.iter_bytes(chunk_size=chunk_size):
                if chunk:
                    tmp.write(chunk)
            tmp.flush()

        @retry(
            retry=retry_if_exception(_is_retriable_download_exc),
            wait=wait_exponential(multiplier=1, min=1, max=60),
            stop=stop_after_attempt(5),
            reraise=True,
        )
        def _request_and_write() -> None:
            try:
                headers = {"User-Agent": "cam/cfpb"}
                if client is not None:
                    cm = client.stream(
                        "GET",
                        url,
                        timeout=timeout,
                        follow_redirects=True,
                        headers=headers,
                    )
                else:
                    cm = httpx.stream(
                        "GET",
                        url,
                        timeout=timeout,
                        follow_redirects=True,
                        headers=headers,
                    )

                with cm as resp:
                    resp.raise_for_status()
                    _write_chunked_response(resp)
            except BaseException:
                # Ensure retries start from a clean temp file.
                tmp.seek(0)
                tmp.truncate(0)
                raise

        try:
            _request_and_write()
            return tmp_path
        except BaseException:
            try:
                tmp_path.unlink(missing_ok=True)
            finally:
                logger.exception("cfpb download failed: %s", url)
            raise


def parse_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if raw == "":
        return None

    # Strict YYYY-MM-DD.
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None

    # ISO timestamp with offset.
    try:
        dt = datetime.fromisoformat(raw)
        return dt.date()
    except ValueError:
        pass

    # US MM/DD/YYYY.
    try:
        return datetime.strptime(raw, "%m/%d/%Y").date()
    except ValueError:
        return None


_WS_RE = re.compile(r"\s+")


def clean_company_name(raw: str | None) -> str:
    if raw is None:
        return ""
    cleaned = raw.replace("\xa0", " ")
    cleaned = _WS_RE.sub(" ", cleaned).strip()
    return cleaned


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = (year * 12 + (month - 1)) + delta
    y = idx // 12
    m = (idx % 12) + 1
    return y, m


def window_months(today: date, n: int = WINDOW_MONTHS) -> list[str]:
    # Exclude current month, end with the month before.
    end_year, end_month = _add_months(today.year, today.month, -1)
    start_year, start_month = _add_months(end_year, end_month, -(n - 1))

    months: list[str] = []
    y, m = start_year, start_month
    for _ in range(n):
        months.append(f"{y:04d}-{m:02d}")
        y, m = _add_months(y, m, 1)
    return months


def _first_csv_member(zf: zipfile.ZipFile) -> str:
    for info in zf.infolist():
        if info.filename.lower().endswith(".csv"):
            return info.filename
    raise ValueError("zip holds no .csv member")


def aggregate(
    zip_path: Path,
    *,
    months: list[str],
) -> tuple[Counter[tuple[str, str, str]], int]:
    counts: Counter[tuple[str, str, str]] = Counter()
    months_set = set(months)
    rows_read = 0

    with zipfile.ZipFile(zip_path) as zf:
        member = _first_csv_member(zf)
        with zf.open(member) as member_fp:
            text_fp = io.TextIOWrapper(member_fp, encoding="utf-8-sig")
            reader = csv.DictReader(text_fp)
            for row in reader:
                rows_read += 1
                if not (row.get("Complaint ID") or "").strip():
                    continue
                dt = parse_date(row.get("Date received"))
                if dt is None:
                    continue
                month = dt.strftime("%Y-%m")
                if month not in months_set:
                    continue
                company = clean_company_name(row.get("Company"))
                if company == "":
                    continue
                product_raw = row.get("Product") or ""
                product = product_raw.strip() or "Unknown"
                counts[(company, month, product)] += 1

    return counts, rows_read


def to_rows(
    counts: Counter[tuple[str, str, str]],
    *,
    min_complaints: int = MIN_COMPLAINTS,
) -> list[dict[str, str]]:
    company_totals: dict[str, int] = {}
    for (company, _month, _product), n in counts.items():
        company_totals[company] = company_totals.get(company, 0) + int(n)

    rows: list[dict[str, str]] = []
    for (company, month, product), n in counts.items():
        if company_totals.get(company, 0) < min_complaints:
            continue
        rows.append(
            {
                "company": company,
                "month": month,
                "product": product,
                "complaints": str(int(n)),
            }
        )
    return rows


__all__ = [
    "BULK_URL",
    "FIELDNAMES",
    "WINDOW_MONTHS",
    "MIN_COMPLAINTS",
    "stream_to_tempfile",
    "parse_date",
    "clean_company_name",
    "window_months",
    "aggregate",
    "to_rows",
]
