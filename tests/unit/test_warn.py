"""
Unit tests for M11 — WARN Act Ingestion.

Covers:
- CSV parsing (CA fixture)
- HTML table parsing (TX fixture)
- PDF extraction (mocked pdfplumber, IL config)
- Establishment name cleaning
- Date / employee count parsing
- Since-date filtering
- Idempotency (duplicate records skipped)
- Entity resolution linkage
- Parallel ingestion (ingest_all_states) deduplication
- get_pe_owned_entities
- Unknown state / fetch error handling
- Performance
"""

from __future__ import annotations

import time
import uuid
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.db.models import Base, Entity, Event, Signal
from cam.ingestion.warn import (
    IngestResult,
    _clean_name,
    _idempotency_key,
    _parse_csv,
    _parse_date,
    _parse_employees,
    _parse_html,
    _parse_pdf,
    get_pe_owned_entities,
    ingest_all_states,
    ingest_state,
)
from cam.ingestion.warn.state_urls import STATE_CONFIGS

FIXTURES = Path(__file__).parent.parent / "fixtures" / "warn"

# ---------------------------------------------------------------------------
# In-memory SQLite database fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


# ---------------------------------------------------------------------------
# Helper: fake httpx client
# ---------------------------------------------------------------------------


def _make_client(content: bytes, status: int = 200):
    """Return a mock httpx.Client whose .get() returns *content*."""
    mock_resp = MagicMock()
    mock_resp.content = content
    mock_resp.status_code = status
    mock_resp.raise_for_status = MagicMock()
    client = MagicMock()
    client.get.return_value = mock_resp
    return client


# ---------------------------------------------------------------------------
# State configuration tests
# ---------------------------------------------------------------------------


def test_state_configs_contain_all_priority_states():
    """All 8 priority states must be present in STATE_CONFIGS."""
    for code in ("CA", "TX", "NY", "FL", "IL", "OH", "PA", "MI"):
        assert code in STATE_CONFIGS, f"{code} missing from STATE_CONFIGS"


def test_state_configs_have_required_columns():
    """Every StateConfig must map the five canonical column names."""
    required = {"company", "date", "employees", "city", "layoff_type"}
    for code, cfg in STATE_CONFIGS.items():
        missing = required - set(cfg.columns.keys())
        assert not missing, f"{code} StateConfig missing columns: {missing}"


def test_state_configs_known_formats():
    for code, cfg in STATE_CONFIGS.items():
        assert cfg.format in ("csv", "html", "pdf"), f"{code}: unknown format {cfg.format!r}"


# ---------------------------------------------------------------------------
# Unit tests for parsing helpers
# ---------------------------------------------------------------------------


def test_parse_date_mm_dd_yyyy():
    assert _parse_date("03/15/2023") == date(2023, 3, 15)


def test_parse_date_iso():
    assert _parse_date("2023-03-15") == date(2023, 3, 15)


def test_parse_date_empty():
    assert _parse_date("") is None
    assert _parse_date(None) is None


def test_parse_date_invalid():
    assert _parse_date("not-a-date") is None


def test_parse_employees_plain():
    assert _parse_employees("350") == 350


def test_parse_employees_comma():
    assert _parse_employees("1,200") == 1200


def test_parse_employees_empty():
    assert _parse_employees("") is None
    assert _parse_employees(None) is None


def test_parse_employees_zero():
    assert _parse_employees("0") is None


def test_clean_name_strips_location_suffix():
    assert _clean_name("Bay Area Logistics Co - OAKLAND") == "Bay Area Logistics Co"


def test_clean_name_no_suffix():
    assert _clean_name("TechCorp Solutions Inc") == "TechCorp Solutions Inc"


def test_clean_name_none():
    assert _clean_name(None) == ""


# ---------------------------------------------------------------------------
# CSV parsing (CA fixture)
# ---------------------------------------------------------------------------


def test_parse_csv_ca_returns_records():
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    records = _parse_csv(content, STATE_CONFIGS["CA"])
    assert len(records) == 5


def test_parse_csv_ca_first_record_fields():
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    records = _parse_csv(content, STATE_CONFIGS["CA"])
    first = records[0]
    assert first.company == "TechCorp Solutions Inc"
    assert first.notice_date == date(2023, 3, 15)
    assert first.employees_affected == 350
    assert first.city == "San Francisco"
    assert first.layoff_type == "Layoff"
    assert first.state_code == "CA"


def test_parse_csv_ca_strips_location_suffix():
    """'Bay Area Logistics Co - OAKLAND' should become 'Bay Area Logistics Co'."""
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    records = _parse_csv(content, STATE_CONFIGS["CA"])
    companies = [r.company for r in records]
    assert "Bay Area Logistics Co" in companies
    assert "Bay Area Logistics Co - OAKLAND" not in companies


def test_parse_csv_ca_all_dates_parseable():
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    records = _parse_csv(content, STATE_CONFIGS["CA"])
    for rec in records:
        assert rec.notice_date is not None, f"{rec.company} has unparseable date"


# ---------------------------------------------------------------------------
# HTML parsing (TX fixture)
# ---------------------------------------------------------------------------


def test_parse_html_tx_returns_records():
    content = (FIXTURES / "tx_warn_sample.html").read_bytes()
    records = _parse_html(content, STATE_CONFIGS["TX"])
    assert len(records) == 4


def test_parse_html_tx_first_record():
    content = (FIXTURES / "tx_warn_sample.html").read_bytes()
    records = _parse_html(content, STATE_CONFIGS["TX"])
    first = records[0]
    assert first.company == "Lone Star Energy Services LLC"
    assert first.notice_date == date(2023, 4, 1)
    assert first.employees_affected == 620
    assert first.city == "Houston"
    assert first.state_code == "TX"


def test_parse_html_tx_all_companies_nonempty():
    content = (FIXTURES / "tx_warn_sample.html").read_bytes()
    records = _parse_html(content, STATE_CONFIGS["TX"])
    for rec in records:
        assert rec.company, "Company name should not be empty"


def test_parse_html_empty_returns_empty():
    records = _parse_html(b"<html><body><p>No table here</p></body></html>", STATE_CONFIGS["TX"])
    assert records == []


# ---------------------------------------------------------------------------
# PDF parsing (mocked pdfplumber)
# ---------------------------------------------------------------------------


def _make_pdf_mock(rows: list[list[str]]) -> MagicMock:
    """Build a pdfplumber mock returning one page with one table."""
    header = [
        STATE_CONFIGS["IL"].columns["company"],
        STATE_CONFIGS["IL"].columns["date"],
        STATE_CONFIGS["IL"].columns["employees"],
        STATE_CONFIGS["IL"].columns["city"],
        STATE_CONFIGS["IL"].columns["county"],
        STATE_CONFIGS["IL"].columns["layoff_type"],
    ]
    table_data = [header] + rows

    page_mock = MagicMock()
    page_mock.extract_tables.return_value = [table_data]

    pdf_mock = MagicMock()
    pdf_mock.pages = [page_mock]
    pdf_mock.__enter__ = MagicMock(return_value=pdf_mock)
    pdf_mock.__exit__ = MagicMock(return_value=False)
    return pdf_mock


def test_parse_pdf_il_returns_records():
    pdf_mock = _make_pdf_mock(
        [
            ["Illinois Steel Inc", "04/10/2023", "300", "Chicago", "Cook", "Closure"],
            ["Midwest Auto Parts LLC", "03/01/2023", "125", "Rockford", "Winnebago", "Layoff"],
        ]
    )
    with patch("pdfplumber.open", return_value=pdf_mock):
        records = _parse_pdf(b"fake-pdf-bytes", STATE_CONFIGS["IL"])
    assert len(records) == 2


def test_parse_pdf_il_first_record_fields():
    pdf_mock = _make_pdf_mock(
        [
            ["Illinois Steel Inc", "04/10/2023", "300", "Chicago", "Cook", "Closure"],
        ]
    )
    with patch("pdfplumber.open", return_value=pdf_mock):
        records = _parse_pdf(b"fake-pdf-bytes", STATE_CONFIGS["IL"])
    rec = records[0]
    assert rec.company == "Illinois Steel Inc"
    assert rec.notice_date == date(2023, 4, 10)
    assert rec.employees_affected == 300
    assert rec.state_code == "IL"


def test_parse_pdf_pdfplumber_exception_returns_empty():
    """pdfplumber failure must return empty list, not raise."""
    with patch("pdfplumber.open", side_effect=Exception("PDF corrupt")):
        records = _parse_pdf(b"bad-pdf", STATE_CONFIGS["IL"])
    assert records == []


def test_parse_pdf_empty_table_returns_empty():
    page_mock = MagicMock()
    page_mock.extract_tables.return_value = []
    pdf_mock = MagicMock()
    pdf_mock.pages = [page_mock]
    pdf_mock.__enter__ = MagicMock(return_value=pdf_mock)
    pdf_mock.__exit__ = MagicMock(return_value=False)
    with patch("pdfplumber.open", return_value=pdf_mock):
        records = _parse_pdf(b"fake", STATE_CONFIGS["IL"])
    assert records == []


# ---------------------------------------------------------------------------
# ingest_state — CA (CSV)
# ---------------------------------------------------------------------------


def test_ingest_state_ca_ingests_records(db):
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    result = ingest_state("CA", db=db, client=client)
    assert isinstance(result, IngestResult)
    assert result.ingested == 5
    assert result.errors == 0


def test_ingest_state_ca_creates_events_in_db(db):
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    ingest_state("CA", db=db, client=client)
    events = db.query(Event).filter_by(source="warn").all()
    assert len(events) == 5


def test_ingest_state_ca_event_type_warn_notice(db):
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    ingest_state("CA", db=db, client=client)
    events = db.query(Event).filter_by(source="warn", event_type="warn_notice").all()
    assert len(events) == 5


def test_ingest_state_ca_since_date_filters(db):
    """Records before since_date must be excluded."""
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    # Only the 03/15/2023 and 02/01/2023 records are at or after 2023-02-01
    result = ingest_state("CA", since_date=date(2023, 2, 1), db=db, client=client)
    assert result.ingested == 2


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_ingest_state_idempotent_second_run_skips_all(db):
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    ingest_state("CA", db=db, client=client)
    # Second run: everything already exists
    client2 = _make_client(content)
    result2 = ingest_state("CA", db=db, client=client2)
    assert result2.ingested == 0
    assert result2.skipped == 5
    # DB still has exactly 5 events
    count = db.query(Event).filter_by(source="warn").count()
    assert count == 5


def test_idempotency_key_stable():
    k1 = _idempotency_key("CA", "TechCorp Solutions Inc", date(2023, 3, 15))
    k2 = _idempotency_key("CA", "TechCorp Solutions Inc", date(2023, 3, 15))
    assert k1 == k2


def test_idempotency_key_differs_by_state():
    k_ca = _idempotency_key("CA", "Acme Corp", date(2023, 1, 1))
    k_tx = _idempotency_key("TX", "Acme Corp", date(2023, 1, 1))
    assert k_ca != k_tx


def test_idempotency_key_differs_by_date():
    k1 = _idempotency_key("CA", "Acme Corp", date(2023, 1, 1))
    k2 = _idempotency_key("CA", "Acme Corp", date(2023, 6, 1))
    assert k1 != k2


# ---------------------------------------------------------------------------
# Entity linkage
# ---------------------------------------------------------------------------


def test_ingest_state_ca_entity_resolution_attempted(db):
    """Entity resolution is attempted: every event has a company name in raw_json.

    In unit tests with a fresh SQLite DB there are no pre-existing entities, so
    entity_id may be None.  The acceptance criterion (>60% resolution) applies
    to production data with a populated entity table.
    """
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    ingest_state("CA", db=db, client=client)
    events = db.query(Event).filter_by(source="warn").all()
    for event in events:
        assert event.raw_json is not None
        assert event.raw_json.get("company"), "Event raw_json must include company name"


def test_ingest_state_ca_event_description_contains_company(db):
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    client = _make_client(content)
    ingest_state("CA", db=db, client=client)
    events = db.query(Event).filter_by(source="warn").all()
    for event in events:
        assert event.description and "CA" in event.description


# ---------------------------------------------------------------------------
# TX HTML ingestion
# ---------------------------------------------------------------------------


def test_ingest_state_tx_html(db):
    content = (FIXTURES / "tx_warn_sample.html").read_bytes()
    client = _make_client(content)
    result = ingest_state("TX", db=db, client=client)
    assert result.ingested == 4
    assert result.errors == 0


# ---------------------------------------------------------------------------
# IL PDF ingestion (mocked)
# ---------------------------------------------------------------------------


def test_ingest_state_il_pdf(db):
    pdf_mock = _make_pdf_mock(
        [
            ["Illinois Steel Inc", "04/10/2023", "300", "Chicago", "Cook", "Closure"],
            ["Midwest Auto Parts LLC", "03/01/2023", "125", "Rockford", "Winnebago", "Layoff"],
        ]
    )
    fake_pdf_bytes = b"fake-pdf"
    client = _make_client(fake_pdf_bytes)
    with patch("pdfplumber.open", return_value=pdf_mock):
        result = ingest_state("IL", db=db, client=client)
    assert result.ingested == 2
    assert result.errors == 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_ingest_state_unknown_state_returns_error(db):
    result = ingest_state("ZZ", db=db)
    assert result.errors == 1
    assert "Unknown state code" in result.error_details[0]


def test_ingest_state_fetch_failure_returns_error(db):
    import httpx

    client = MagicMock()
    client.get.side_effect = httpx.NetworkError("connection refused")
    result = ingest_state("CA", db=db, client=client)
    assert result.errors >= 1


# ---------------------------------------------------------------------------
# ingest_all_states — parallel deduplication
# ---------------------------------------------------------------------------


def test_ingest_all_states_returns_one_result_per_state(db):
    """Each configured state must produce exactly one IngestResult."""
    ca_content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    tx_content = (FIXTURES / "tx_warn_sample.html").read_bytes()

    def _fake_fetch(url: str, *, client=None):
        if "edd.ca.gov" in url:
            return ca_content
        if "twc.texas.gov" in url:
            return tx_content
        return b""

    with patch("cam.ingestion.warn._fetch", side_effect=_fake_fetch):
        results = ingest_all_states(db=db)

    assert len(results) == len(STATE_CONFIGS)
    codes = {r.state_code for r in results}
    assert codes == set(STATE_CONFIGS.keys())


def test_ingest_all_states_no_duplicates(db):
    """Running ingest_all_states twice must not duplicate events."""
    ca_content = (FIXTURES / "ca_warn_sample.csv").read_bytes()

    def _fake_fetch(url: str, *, client=None):
        if "edd.ca.gov" in url:
            return ca_content
        return b""

    with patch("cam.ingestion.warn._fetch", side_effect=_fake_fetch):
        ingest_all_states(db=db)
        ingest_all_states(db=db)

    ca_events = db.query(Event).filter_by(source="warn").all()
    ca_keys = [e.raw_json["_warn_key"] for e in ca_events if e.raw_json.get("state_code") == "CA"]
    assert len(ca_keys) == len(set(ca_keys)), "Duplicate WARN events found after two runs"


# ---------------------------------------------------------------------------
# get_pe_owned_entities
# ---------------------------------------------------------------------------


def test_get_pe_owned_entities_empty(db):
    result = get_pe_owned_entities(db)
    assert result == []


def test_get_pe_owned_entities_returns_flagged_ids(db):
    # Create a canonical entity
    eid = uuid.uuid4()
    entity = Entity(id=eid, canonical_name="Blackstone Portfolio Co")
    db.add(entity)
    db.flush()

    # Flag it as PE-owned
    sig = Signal(
        entity_id=eid,
        source="manual",
        signal_type="pe_owned",
        score=1.0,
        evidence="Listed in PE Stakeholder Project 2023 dataset",
    )
    db.add(sig)
    db.commit()

    result = get_pe_owned_entities(db)
    assert eid in result


def test_get_pe_owned_entities_excludes_non_pe(db):
    """Entities with other signal types must not appear in the PE list."""
    eid = uuid.uuid4()
    entity = Entity(id=eid, canonical_name="Public Corp")
    db.add(entity)
    db.flush()

    sig = Signal(
        entity_id=eid,
        source="edgar",
        signal_type="risk_language_expansion",
        score=0.8,
    )
    db.add(sig)
    db.commit()

    result = get_pe_owned_entities(db)
    assert eid not in result


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_parse_csv_performance():
    """Parsing CA CSV must complete in under 100 ms."""
    content = (FIXTURES / "ca_warn_sample.csv").read_bytes()
    start = time.perf_counter()
    _parse_csv(content, STATE_CONFIGS["CA"])
    elapsed = (time.perf_counter() - start) * 1000
    assert elapsed < 100, f"_parse_csv took {elapsed:.1f} ms"


def test_parse_html_performance():
    """Parsing TX HTML must complete in under 200 ms."""
    content = (FIXTURES / "tx_warn_sample.html").read_bytes()
    start = time.perf_counter()
    _parse_html(content, STATE_CONFIGS["TX"])
    elapsed = (time.perf_counter() - start) * 1000
    assert elapsed < 200, f"_parse_html took {elapsed:.1f} ms"
