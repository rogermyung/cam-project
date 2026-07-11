"""
Authoritative tests for the California WARN XLSX parser (M11 — Phase 4).

California switched its WARN report from CSV to a multi-sheet XLSX workbook
in early 2026 (``warn_report1.xlsx``).  The detailed data lives on a sheet
named ``"Detailed WARN Report "`` (note the trailing space) whose real header
row is the *second* row of the sheet — the first row is a title banner that
spans the table.  Header cells contain embedded newlines (e.g. ``"Notice\nDate"``,
``"No. Of\nEmployees"``, ``"Layoff/\nClosure"``).

``parse_ca_xlsx(content: bytes) -> list[WarnRecord]`` must handle all of the
above and return one WarnRecord per data row.  These assertions are pinned to
the committed fixture ``tests/fixtures/warn/ca_warn_report.xlsx`` (a trimmed
copy of the real state workbook).
"""

from __future__ import annotations

import io
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from cam.ingestion.warn import WarnRecord
from cam.ingestion.warn._xlsx import (
    CaXlsxStructureError,
    _parse_xlsx_employees,
    parse_ca_xlsx,
)

FIXTURE = Path(__file__).parent.parent / "fixtures" / "warn" / "ca_warn_report.xlsx"


def _load() -> list[WarnRecord]:
    return parse_ca_xlsx(FIXTURE.read_bytes())


def test_returns_all_nine_data_rows():
    records = _load()
    assert len(records) == 9


def test_returns_warnrecord_instances_tagged_ca():
    for rec in _load():
        assert isinstance(rec, WarnRecord)
        assert rec.state_code == "CA"


def test_every_record_has_nonempty_company():
    for rec in _load():
        assert rec.company, f"empty company in {rec!r}"


def test_first_record_fields():
    rec = _load()[0]
    assert rec.company == "McDonald's Corporation"
    assert rec.notice_date == date(2026, 6, 30)
    assert rec.employees_affected == 2
    assert "Los Angeles" in rec.county
    assert "Closure" in rec.layoff_type


def test_employee_counts_parsed_as_ints():
    counts = [r.employees_affected for r in _load()]
    # Genentech 103, Chevron 180, LeeMAH 212 are all present and integer-typed.
    assert 103 in counts
    assert 180 in counts
    assert 212 in counts
    assert all(isinstance(c, int) for c in counts)


def test_notice_dates_parsed_for_all_rows():
    for rec in _load():
        assert isinstance(rec.notice_date, date)


def test_raw_is_json_serializable():
    import json

    # raw is persisted into Event.raw_json, so every value must be
    # JSON-serializable — datetime cells must be coerced, not stored raw.
    for rec in _load():
        json.dumps(rec.raw)


def test_company_with_parenthetical_preserved():
    companies = {r.company for r in _load()}
    # Company names must not be truncated at punctuation.
    assert any(c.startswith("Kingdom Animalia") for c in companies)
    assert "Chevron" in companies


def test_employee_count_accepts_integral_float():
    # openpyxl commonly types numeric cells as float; integral floats must be
    # kept (15.0 -> 15), non-integral rejected.
    assert _parse_xlsx_employees(15.0) == 15
    assert _parse_xlsx_employees(1200.0) == 1200
    assert _parse_xlsx_employees(0.0) is None
    assert _parse_xlsx_employees(3.5) is None
    assert _parse_xlsx_employees(True) is None  # bool is not a count


def _workbook_bytes(sheet_name: str, rows: list[list]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_missing_sheet_raises_structure_error():
    # Upstream drift (renamed/removed sheet) must be a loud parse error, not a
    # silent empty result that reads as "0 notices this week".
    content = _workbook_bytes("Some Other Sheet", [["banner"], ["Company"], ["X"]])
    with pytest.raises(CaXlsxStructureError):
        parse_ca_xlsx(content)


def test_missing_required_column_raises_structure_error():
    # Header present but the Company column is gone -> structural failure.
    content = _workbook_bytes(
        "Detailed WARN Report",
        [["banner"], ["County/Parish", "Notice Date", "No. Of Employees"], ["LA", "01/01/2026", 5]],
    )
    with pytest.raises(CaXlsxStructureError):
        parse_ca_xlsx(content)


def test_valid_sheet_with_no_data_rows_returns_empty():
    # A valid layout with zero notices is legitimate, not an error.
    content = _workbook_bytes(
        "Detailed WARN Report",
        [["banner"], ["Company", "Notice Date", "No. Of Employees"]],
    )
    assert parse_ca_xlsx(content) == []
