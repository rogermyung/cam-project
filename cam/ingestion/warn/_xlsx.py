"""Parser for California WARN Act XLSX workbooks (Phase 4)."""

from __future__ import annotations

import io
import re
from datetime import date, datetime
from typing import Any

import openpyxl

from cam.ingestion.warn import WarnRecord


def _normalize_header(raw: str) -> str:
    """Normalize a header by lowercasing and collapsing whitespace."""
    return re.sub(r"\s+", " ", raw.lower().strip())


def _parse_xlsx_date(value: Any) -> date | None:
    """Parse a date from an XLSX cell value.

    Handles:
    - datetime objects (from data_only=True with date formatting)
    - date objects
    - strings (MM/DD/YYYY or YYYY-MM-DD format)
    """
    if value is None:
        return None

    # If already a datetime or date object
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    # Try to parse as string
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue

    return None


def _parse_xlsx_employees(value: Any) -> int | None:
    """Parse employee count from an XLSX cell value.

    Handles:
    - int values
    - strings like '1,200'
    """
    if value is None:
        return None

    # If already an int
    if isinstance(value, int):
        return value if value > 0 else None

    # Try to parse as string
    if isinstance(value, str):
        raw = value.strip().replace(",", "")
        try:
            n = int(raw)
            return n if n > 0 else None
        except ValueError:
            return None

    return None


def parse_ca_xlsx(content: bytes) -> list[WarnRecord]:
    """Parse a California WARN Act XLSX workbook.

    The workbook contains a sheet named 'Detailed WARN Report ' (with trailing space).
    Row 1 is a title banner; row 2 is the header; data starts at row 3.
    Header cells contain embedded newlines; they are matched by normalizing
    (lowercase, collapse whitespace).

    Parameters
    ----------
    content : bytes
        The raw XLSX file content.

    Returns
    -------
    list[WarnRecord]
        List of parsed WARN records.
    """
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)

    # Find the correct sheet by normalized name
    sheet = None
    for name in wb.sheetnames:
        if _normalize_header(name) == "detailed warn report":
            sheet = wb[name]
            break

    if sheet is None:
        return []

    records: list[WarnRecord] = []
    rows = list(sheet.iter_rows(values_only=True))

    if len(rows) < 2:
        return []

    # Row 2 is the header (0-indexed: rows[1])
    header_row = rows[1]
    if not header_row:
        return []

    # Build column index map: normalized_header -> column_index
    col_index: dict[str, int] = {}
    for col_idx, cell_value in enumerate(header_row):
        if cell_value is not None:
            normalized = _normalize_header(str(cell_value))
            col_index[normalized] = col_idx

    # Data rows start at row 3 (0-indexed: rows[2] onward)
    for row_data in rows[2:]:
        # Stop at first fully empty row (all cells are None)
        if all(v is None for v in row_data):
            break

        # Extract fields using normalized header keys
        raw_company = None
        if "company" in col_index:
            raw_company = row_data[col_index["company"]]

        # Skip rows with empty company
        if not raw_company:
            continue

        company = str(raw_company).strip() if raw_company else ""

        notice_date = None
        if "notice date" in col_index:
            notice_date = _parse_xlsx_date(row_data[col_index["notice date"]])

        employees_affected = None
        if "no. of employees" in col_index:
            employees_affected = _parse_xlsx_employees(row_data[col_index["no. of employees"]])

        city = ""
        if "address" in col_index:
            city_val = row_data[col_index["address"]]
            city = str(city_val).strip() if city_val else ""

        county = ""
        if "county/parish" in col_index:
            county_val = row_data[col_index["county/parish"]]
            county = str(county_val).strip() if county_val else ""

        layoff_type = ""
        if "layoff/ closure" in col_index:
            layoff_val = row_data[col_index["layoff/ closure"]]
            layoff_type = str(layoff_val).strip() if layoff_val else ""

        # Build raw dict from all columns.  Values must be JSON-serializable
        # because they are persisted into Event.raw_json (and the DLQ) — coerce
        # datetime/date cells to ISO strings rather than storing raw objects.
        raw: dict[str, Any] = {}
        for header, col_idx in col_index.items():
            if col_idx < len(row_data):
                cell = row_data[col_idx]
                raw[header] = cell.isoformat() if isinstance(cell, (datetime, date)) else cell

        record = WarnRecord(
            state_code="CA",
            company=company,
            notice_date=notice_date,
            employees_affected=employees_affected,
            city=city,
            county=county,
            layoff_type=layoff_type,
            raw=raw,
        )
        records.append(record)

    return records
