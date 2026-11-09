"""Authoritative tests for the database-free WARN package ``cam.warn``.

``cam.warn`` is a port of the parsing half of ``cam.ingestion.warn`` with every
database, entity-resolution, circuit-breaker and settings dependency removed.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import httpx
import pytest

from cam.warn import (
    FIELDNAMES,
    WarnRecord,
    fetch,
    parse,
    record_key,
    record_to_row,
)
from cam.warn.state_urls import STATE_CONFIGS, StateConfig

FIXTURES = Path(__file__).parent.parent / "fixtures" / "warn"

CSV_CFG = StateConfig(
    state_code="CA",
    url="https://example.test/ca.csv",
    format="csv",
    columns={
        "company": "Company",
        "date": "Notice Date",
        "employees": "No. Of Employees Affected",
        "city": "City",
        "county": "County",
        "layoff_type": "Event Type",
    },
)


HTML_CFG = StateConfig(
    state_code="TX",
    url="https://example.test/tx.html",
    format="html",
    columns={
        "company": "Company Name",
        "date": "Layoff Date",
        "employees": "Number of Employees",
        "city": "City",
        "county": "County",
        "layoff_type": "Type of Layoff",
    },
)


# --- no heavyweight dependencies ------------------------------------------------


def test_package_imports_without_database_or_pipeline_modules():
    code = (
        "import sys\n"
        "import cam.warn, cam.warn._xlsx, cam.warn._mi, cam.warn._pa, cam.warn.state_urls\n"
        "banned_roots = {'sqlalchemy', 'celery', 'redis', 'boto3', 'alembic', 'pdfplumber'}\n"
        "banned_prefixes = ('cam.db', 'cam.entity', 'cam.ingestion', 'cam.config', "
        "'cam.analysis', 'cam.alerts')\n"
        "bad = sorted(m for m in sys.modules if m.split('.')[0] in banned_roots "
        "or m.startswith(banned_prefixes))\n"
        "assert not bad, f'cam.warn must not import: {bad}'\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


# --- state configuration --------------------------------------------------------


def test_only_the_five_working_states_are_configured():
    assert set(STATE_CONFIGS) == {"CA", "TX", "NY", "PA", "MI"}


def test_state_formats():
    assert STATE_CONFIGS["CA"].format == "xlsx"
    assert STATE_CONFIGS["MI"].format == "mi_json"
    assert STATE_CONFIGS["PA"].format == "pa_html"
    for code in ("TX", "NY"):
        assert STATE_CONFIGS[code].format == "csv"
    for code, cfg in STATE_CONFIGS.items():
        assert cfg.state_code == code
        assert cfg.url.startswith("https://")


# --- parse() dispatch -----------------------------------------------------------


def test_parse_csv_fixture():
    records = parse((FIXTURES / "ca_warn_sample.csv").read_bytes(), CSV_CFG)
    assert len(records) == 5
    first = records[0]
    assert isinstance(first, WarnRecord)
    assert first.state_code == "CA"
    assert first.company == "TechCorp Solutions Inc"
    assert first.notice_date == date(2023, 3, 15)
    assert first.employees_affected == 350
    assert first.city == "San Francisco"
    assert first.county == "San Francisco"
    assert first.layoff_type == "Layoff"
    assert first.raw["NAICS"] == "541511"


def test_parse_csv_strips_location_suffix_from_company():
    records = parse((FIXTURES / "ca_warn_sample.csv").read_bytes(), CSV_CFG)
    names = [r.company for r in records]
    assert "Bay Area Logistics Co" in names
    assert not any("OAKLAND" in n for n in names)


def test_parse_html_fixture():
    records = parse((FIXTURES / "tx_warn_sample.html").read_bytes(), HTML_CFG)
    assert len(records) == 4
    assert records[0].company == "Lone Star Energy Services LLC"
    assert records[0].notice_date == date(2023, 4, 1)
    assert records[0].employees_affected == 620
    assert records[0].city == "Houston"
    assert records[0].county == "Harris"
    assert records[1].layoff_type == "Closure"
    assert records[-1].company == "Gulf Coast Petrochemical Ltd"


def test_parse_html_without_table_returns_empty_list():
    assert parse(b"<html><body><p>moved</p></body></html>", HTML_CFG) == []


def test_parse_xlsx_fixture_via_ca_config():
    records = parse((FIXTURES / "ca_warn_report.xlsx").read_bytes(), STATE_CONFIGS["CA"])
    assert len(records) == 9
    assert records[0].company == "McDonald's Corporation"
    assert records[0].notice_date == date(2026, 6, 30)
    assert records[0].employees_affected == 2


def test_parse_mi_fixture_via_mi_config():
    records = parse((FIXTURES / "mi_warn_sample.json").read_bytes(), STATE_CONFIGS["MI"])
    assert len(records) == 6
    assert records[0].company == "Conduent Commercial Solutions, LLC"
    assert records[0].notice_date == date(2026, 8, 28)
    assert records[0].employees_affected == 15


@pytest.mark.parametrize("fmt", ["pdf", "nope"])
def test_parse_rejects_unsupported_formats(fmt):
    cfg = StateConfig(state_code="ZZ", url="https://example.test", format=fmt)
    with pytest.raises(ValueError):
        parse(b"", cfg)


# --- record_key -----------------------------------------------------------------


def _rec(**kw) -> WarnRecord:
    base = dict(
        state_code="TX",
        company="Acme Corp",
        notice_date=date(2026, 1, 2),
        employees_affected=100,
        city="Austin",
        county="Travis",
        layoff_type="Layoff",
        raw={"a": "1"},
    )
    base.update(kw)
    return WarnRecord(**base)


def test_record_key_format():
    assert record_key(_rec()) == "TX::acme corp::2026-01-02::austin"


def test_record_key_is_case_and_whitespace_insensitive():
    assert record_key(_rec(company="  ACME corp ", city=" AUSTIN ")) == record_key(_rec())


def test_record_key_distinguishes_sites_of_same_employer_on_same_day():
    assert record_key(_rec(city="Austin")) != record_key(_rec(city="Dallas"))


def test_record_key_without_date_hashes_raw_fields():
    a = record_key(_rec(notice_date=None, raw={"row": "1"}))
    b = record_key(_rec(notice_date=None, raw={"row": "2"}))
    assert a != b
    assert a.startswith("TX::acme corp::no-date:")
    assert a.endswith("::austin")
    assert a == record_key(_rec(notice_date=None, raw={"row": "1"}))


# --- record_to_row --------------------------------------------------------------


def test_fieldnames_are_the_csv_schema():
    assert FIELDNAMES == [
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


def test_record_to_row_maps_every_field_to_a_string():
    row = record_to_row(_rec(), source_url="https://example.test/tx", first_seen=date(2026, 11, 9))
    assert row == {
        "key": "TX::acme corp::2026-01-02::austin",
        "state": "TX",
        "employer": "Acme Corp",
        "notice_date": "2026-01-02",
        "employees": "100",
        "city": "Austin",
        "county": "Travis",
        "notice_type": "Layoff",
        "source_url": "https://example.test/tx",
        "first_seen": "2026-11-09",
    }


def test_record_to_row_uses_empty_strings_for_missing_values():
    row = record_to_row(
        _rec(notice_date=None, employees_affected=None),
        source_url="u",
        first_seen=date(2026, 11, 9),
    )
    assert row["notice_date"] == ""
    assert row["employees"] == ""
    assert list(row) == FIELDNAMES


def test_record_to_row_collapses_whitespace_and_newlines():
    row = record_to_row(
        _rec(city="1 Main St\n  Detroit,\xa0MI"), source_url="u", first_seen=date(2026, 11, 9)
    )
    assert row["city"] == "1 Main St Detroit, MI"


# --- fetch ----------------------------------------------------------------------


def test_fetch_returns_body_and_sends_browser_user_agent():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ua"] = request.headers.get("user-agent", "")
        seen["url"] = str(request.url)
        return httpx.Response(200, content=b"payload")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert fetch("https://example.test/warn", client=client) == b"payload"
    assert seen["url"] == "https://example.test/warn"
    assert "Mozilla" in seen["ua"]


def test_fetch_raises_immediately_on_404():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            fetch("https://example.test/gone", client=client)
    assert calls == 1
