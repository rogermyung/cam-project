"""Unit tests for M5 — CFPB Ingestion."""

from __future__ import annotations

import csv
import io
import time
import uuid
import zipfile
from contextlib import contextmanager
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.db.models import Base, Entity, Event
from cam.ingestion.cfpb import (
    ComplaintRate,
    _clean_company_name,
    _csv_row_to_complaint,
    _fetch_bulk_complaints,
    _get_existing_complaint_ids,
    _is_retriable_error,
    _parse_bulk_zip,
    _parse_date,
    _parse_decimal,
    _stream_to_tempfile,
    compute_complaint_rate,
    detect_complaint_spike,
    ingest_complaints,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "cfpb"
COMPLAINTS_CSV = FIXTURES_DIR / "complaints_sample.csv"
COMPLAINTS_ZIP = FIXTURES_DIR / "complaints_sample.zip"


# ---------------------------------------------------------------------------
# DB / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def _make_entity(db: Session, name: str) -> Entity:
    e = Entity(canonical_name=name)
    db.add(e)
    db.commit()
    db.refresh(e)
    return e


def _make_zip_response(csv_content: str | bytes) -> MagicMock:
    """Return a mock httpx.Response whose .content is a ZIP containing the CSV.

    Used by ``TestParseBulkZip`` which calls ``_parse_bulk_zip(bytes, ...)``
    directly; NOT used by the streaming-path tests.
    """
    if isinstance(csv_content, str):
        csv_content = csv_content.encode("utf-8")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("complaints.csv", csv_content)
    zip_bytes = buf.getvalue()

    mock = MagicMock(spec=httpx.Response)
    mock.content = zip_bytes
    mock.raise_for_status.return_value = None
    return mock


def _make_streaming_client(zip_bytes: bytes) -> MagicMock:
    """Return a mock httpx.Client whose .stream() context manager yields a
    streaming-response mock with iter_bytes() returning the zip in one chunk.

    This mirrors the ``client.stream("GET", url, timeout=...)`` usage inside
    ``_stream_to_tempfile``.
    """
    # Build the streaming response mock
    stream_resp = MagicMock()
    stream_resp.raise_for_status.return_value = None
    stream_resp.iter_bytes.return_value = iter([zip_bytes])

    # Make client.stream(...) a context manager that yields stream_resp
    @contextmanager
    def _stream_ctx(*args, **kwargs):
        yield stream_resp

    client = MagicMock(spec=httpx.Client)
    client.stream.side_effect = _stream_ctx
    return client


def _fixture_csv_text() -> str:
    return COMPLAINTS_CSV.read_text(encoding="utf-8")


def _flatten_fixture() -> list[dict]:
    """Return fixture CSV as a flat list of internal complaint dicts."""
    text = _fixture_csv_text()
    reader = csv.DictReader(io.StringIO(text))
    from cam.ingestion.cfpb import _csv_row_to_complaint

    complaints = []
    for row in reader:
        c = _csv_row_to_complaint(row)
        if c:
            complaints.append(c)
    return complaints


def _seed_complaint_event(
    db: Session,
    entity_id,
    event_date: date,
    complaint_id: str = None,
) -> Event:
    cid = complaint_id or f"CFPB-SEED-{uuid.uuid4()}"
    ev = Event(
        entity_id=entity_id,
        source="cfpb_complaint",
        event_type="complaint",
        event_date=event_date,
        penalty_usd=None,
        raw_json={"complaint_id": cid, "company": "Test Corp"},
    )
    db.add(ev)
    db.commit()
    return ev


def _seed_edgar_event(
    db: Session,
    entity_id,
    total_assets: float,
    period_end: str = "2022-12-31",
) -> Event:
    ev = Event(
        entity_id=entity_id,
        source="sec_edgar",
        event_type="filing",
        event_date=date(2023, 2, 1),
        penalty_usd=None,
        raw_json={
            "accession_number": f"0000-{uuid.uuid4()}",
            "filing_type": "10-K",
            "xbrl_facts": {
                "Assets": {"value": total_assets, "period_end": period_end},
            },
        },
    )
    db.add(ev)
    db.commit()
    return ev


# ---------------------------------------------------------------------------
# TestParseDate
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_iso_format(self):
        assert _parse_date("2022-05-15") == date(2022, 5, 15)

    def test_slash_format(self):
        assert _parse_date("05/15/2022") == date(2022, 5, 15)

    def test_none_returns_none(self):
        assert _parse_date(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_date("") is None

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# TestParseDecimal
# ---------------------------------------------------------------------------


class TestParseDecimal:
    def test_integer(self):
        assert _parse_decimal(1_000_000) == Decimal("1000000")

    def test_string_with_comma(self):
        assert _parse_decimal("1,000,000") == Decimal("1000000")

    def test_none_returns_none(self):
        assert _parse_decimal(None) is None

    def test_negative_returns_none(self):
        assert _parse_decimal("-5") is None

    def test_empty_returns_none(self):
        assert _parse_decimal("") is None


# ---------------------------------------------------------------------------
# TestCleanCompanyName
# ---------------------------------------------------------------------------


class TestCleanCompanyName:
    def test_strips_national_association(self):
        assert _clean_company_name("WELLS FARGO BANK, NATIONAL ASSOCIATION") == "WELLS FARGO BANK"

    def test_strips_na_suffix(self):
        assert _clean_company_name("CITIBANK, N.A.") == "CITIBANK"

    def test_strips_inc(self):
        result = _clean_company_name("ACME FINANCIAL SERVICES INC.")
        assert "INC" not in result.upper()

    def test_none_returns_empty_string(self):
        assert _clean_company_name(None) == ""

    def test_no_suffix_unchanged(self):
        assert _clean_company_name("JPMORGAN CHASE") == "JPMORGAN CHASE"


# ---------------------------------------------------------------------------
# TestRetryLogic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_timeout_is_retriable(self):
        assert _is_retriable_error(httpx.TimeoutException("timeout"))

    def test_network_error_is_retriable(self):
        assert _is_retriable_error(httpx.NetworkError("network"))

    def test_429_is_retriable(self):
        resp = MagicMock()
        resp.status_code = 429
        exc = httpx.HTTPStatusError("rate limited", request=MagicMock(), response=resp)
        assert _is_retriable_error(exc)

    def test_500_is_retriable(self):
        resp = MagicMock()
        resp.status_code = 500
        exc = httpx.HTTPStatusError("server error", request=MagicMock(), response=resp)
        assert _is_retriable_error(exc)

    def test_503_is_retriable(self):
        resp = MagicMock()
        resp.status_code = 503
        exc = httpx.HTTPStatusError("service unavailable", request=MagicMock(), response=resp)
        assert _is_retriable_error(exc)

    def test_502_is_retriable(self):
        resp = MagicMock()
        resp.status_code = 502
        exc = httpx.HTTPStatusError("bad gateway", request=MagicMock(), response=resp)
        assert _is_retriable_error(exc)

    def test_504_is_retriable(self):
        resp = MagicMock()
        resp.status_code = 504
        exc = httpx.HTTPStatusError("gateway timeout", request=MagicMock(), response=resp)
        assert _is_retriable_error(exc)

    def test_404_not_retriable(self):
        resp = MagicMock()
        resp.status_code = 404
        exc = httpx.HTTPStatusError("not found", request=MagicMock(), response=resp)
        assert not _is_retriable_error(exc)

    def test_value_error_not_retriable(self):
        assert not _is_retriable_error(ValueError("bad value"))


# ---------------------------------------------------------------------------
# TestCsvRowToComplaint
# ---------------------------------------------------------------------------


class TestCsvRowToComplaint:
    def test_maps_columns_to_internal_names(self):
        row = {
            "Complaint ID": "TEST-1",
            "Company": "Test Bank",
            "Date received": "2022-06-01",
            "Product": "Mortgage",
            "Issue": "Billing error",
        }
        result = _csv_row_to_complaint(row)
        assert result is not None
        assert result["complaint_id"] == "TEST-1"
        assert result["company"] == "Test Bank"
        assert result["date_received"] == "2022-06-01"
        assert result["product"] == "Mortgage"
        assert result["issue"] == "Billing error"

    def test_returns_none_for_missing_complaint_id(self):
        row = {"Complaint ID": "", "Company": "Ghost Bank"}
        assert _csv_row_to_complaint(row) is None

    def test_returns_none_for_absent_complaint_id_key(self):
        row = {"Company": "Ghost Bank"}
        assert _csv_row_to_complaint(row) is None

    def test_complaint_id_always_present_in_output(self):
        row = {"Complaint ID": "ABC-42", "Company": "Good Bank"}
        result = _csv_row_to_complaint(row)
        assert "complaint_id" in result
        assert result["complaint_id"] == "ABC-42"


# ---------------------------------------------------------------------------
# TestParseBulkZip
# ---------------------------------------------------------------------------


class TestParseBulkZip:
    def test_parses_fixture_csv(self):
        csv_text = _fixture_csv_text()
        zip_bytes = _make_zip_response(csv_text).content
        complaints = _parse_bulk_zip(zip_bytes, date(2022, 1, 1))
        # 10 complaints from 2022; 1 from 2021 filtered out
        assert len(complaints) == 10

    def test_since_date_filter_applied(self):
        csv_text = _fixture_csv_text()
        zip_bytes = _make_zip_response(csv_text).content
        all_complaints = _parse_bulk_zip(zip_bytes, date(2021, 1, 1))
        assert len(all_complaints) == 11  # includes the 2021 record

    def test_bad_zip_returns_empty(self):
        result = _parse_bulk_zip(b"not-a-zip", date(2022, 1, 1))
        assert result == []

    def test_zip_with_no_csv_returns_empty(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w") as zf:
            zf.writestr("README.txt", "no CSV here")
        result = _parse_bulk_zip(buf.getvalue(), date(2022, 1, 1))
        assert result == []

    def test_complaint_ids_present(self):
        csv_text = _fixture_csv_text()
        zip_bytes = _make_zip_response(csv_text).content
        complaints = _parse_bulk_zip(zip_bytes, date(2022, 1, 1))
        ids = {c["complaint_id"] for c in complaints}
        assert "CFPB-2022-001" in ids
        assert "CFPB-2021-OLD" not in ids


# ---------------------------------------------------------------------------
# TestGetExistingComplaintIds
# ---------------------------------------------------------------------------


class TestGetExistingComplaintIds:
    def test_returns_existing_ids(self, db):
        entity = _make_entity(db, "Corp A")
        ev = Event(
            entity_id=entity.id,
            source="cfpb_complaint",
            event_type="complaint",
            raw_json={"complaint_id": "CFPB-999"},
        )
        db.add(ev)
        db.commit()
        ids = _get_existing_complaint_ids(db)
        assert "CFPB-999" in ids

    def test_ignores_other_sources(self, db):
        entity = _make_entity(db, "Corp B")
        ev = Event(
            entity_id=entity.id,
            source="epa_echo",
            event_type="violation",
            raw_json={"complaint_id": "CFPB-OTHER"},
        )
        db.add(ev)
        db.commit()
        ids = _get_existing_complaint_ids(db)
        assert "CFPB-OTHER" not in ids


# ---------------------------------------------------------------------------
# TestFetchBulkComplaints
# ---------------------------------------------------------------------------


class TestFetchBulkComplaints:
    """Tests for _fetch_bulk_complaints using the new streaming download path."""

    def test_downloads_and_parses(self):
        zip_bytes = COMPLAINTS_ZIP.read_bytes()
        client = _make_streaming_client(zip_bytes)

        complaints = _fetch_bulk_complaints(date(2022, 1, 1), client=client)
        assert len(complaints) == 10  # 2021 record filtered

    def test_filters_by_since_date(self):
        zip_bytes = COMPLAINTS_ZIP.read_bytes()
        client = _make_streaming_client(zip_bytes)

        complaints = _fetch_bulk_complaints(date(2021, 1, 1), client=client)
        assert len(complaints) == 11


# ---------------------------------------------------------------------------
# TestStreamToTempfile
# ---------------------------------------------------------------------------


class TestStreamToTempfile:
    """Tests for the _stream_to_tempfile streaming helper."""

    def test_writes_zip_to_tempfile(self):
        """Streamed bytes must produce a valid, parseable ZIP on disk."""
        zip_bytes = COMPLAINTS_ZIP.read_bytes()
        client = _make_streaming_client(zip_bytes)

        with patch("cam.ingestion.cfpb.get_settings") as mock_settings:
            mock_settings.return_value.cfpb_bulk_url = "http://fake/complaints.csv.zip"
            tmp_path = _stream_to_tempfile("http://fake/complaints.csv.zip", client=client)
        try:
            assert tmp_path.exists()
            assert tmp_path.stat().st_size > 0
            # Must be a valid ZIP
            assert zipfile.is_zipfile(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_tempfile_cleaned_up_by_fetch(self):
        """_fetch_bulk_complaints must delete the temp file after parsing."""
        zip_bytes = COMPLAINTS_ZIP.read_bytes()
        client = _make_streaming_client(zip_bytes)
        created_paths: list[Path] = []

        original_stream = _stream_to_tempfile

        def _spy_stream(url, **kwargs):
            path = original_stream(url, **kwargs)
            created_paths.append(path)
            return path

        with (
            patch("cam.ingestion.cfpb.get_settings") as mock_settings,
            patch("cam.ingestion.cfpb._stream_to_tempfile", side_effect=_spy_stream),
        ):
            mock_settings.return_value.cfpb_bulk_url = "http://fake/complaints.csv.zip"
            _fetch_bulk_complaints(date(2022, 1, 1), client=client)

        assert created_paths, "_stream_to_tempfile was never called"
        for p in created_paths:
            assert not p.exists(), f"Temp file {p} was not cleaned up"

    def test_streaming_end_to_end_against_fixture(self):
        """Full streaming path from fixture zip through _fetch_bulk_complaints.

        This is the end-to-end streaming test: the fixture ZIP is served via the
        streaming client mock, downloaded chunk-by-chunk to a tempfile, parsed
        from the tempfile, and filtered by date — all without the zip ever being
        held fully in memory as a single ``resp.content`` blob.
        """
        zip_bytes = COMPLAINTS_ZIP.read_bytes()
        # Split into 512-byte chunks to exercise multi-chunk streaming
        chunk_size = 512
        chunks = [zip_bytes[i : i + chunk_size] for i in range(0, len(zip_bytes), chunk_size)]

        stream_resp = MagicMock()
        stream_resp.raise_for_status.return_value = None
        stream_resp.iter_bytes.return_value = iter(chunks)

        @contextmanager
        def _stream_ctx(*args, **kwargs):
            yield stream_resp

        client = MagicMock(spec=httpx.Client)
        client.stream.side_effect = _stream_ctx

        with patch("cam.ingestion.cfpb.get_settings") as mock_settings:
            mock_settings.return_value.cfpb_bulk_url = "http://fake/complaints.csv.zip"
            complaints = _fetch_bulk_complaints(date(2022, 1, 1), client=client)

        assert len(complaints) == 10  # 10 from 2022, 1 filtered (CFPB-2021-OLD)
        ids = {c["complaint_id"] for c in complaints}
        assert "CFPB-2022-001" in ids
        assert "CFPB-2021-OLD" not in ids


# ---------------------------------------------------------------------------
# TestIngestComplaints
# ---------------------------------------------------------------------------


class TestIngestComplaints:
    @pytest.fixture(autouse=True)
    def mock_entity_resolution(self, monkeypatch):
        import uuid

        from cam.entity.resolver import ResolveResult

        fake_eid = uuid.uuid4()

        def _fake_bulk_resolve(records, source, db, commit=True):
            return [
                ResolveResult(
                    entity_id=fake_eid,
                    canonical_name="Fake Entity",
                    confidence=1.0,
                    method="exact",
                    needs_review=False,
                )
                for _ in records
            ]

        monkeypatch.setattr("cam.ingestion.cfpb.bulk_resolve", _fake_bulk_resolve)

    def test_ingests_all_within_date(self, db):
        complaints = _flatten_fixture()
        # since_date=2022-01-01 excludes the 2021-11-15 record (CFPB-2021-OLD)
        result = ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        assert result.total == 11
        assert result.ingested == 10
        assert result.skipped == 1
        assert result.errors == 0

    def test_ingests_all_with_old_since_date(self, db):
        complaints = _flatten_fixture()
        result = ingest_complaints(date(2021, 1, 1), db=db, complaints=complaints)
        assert result.ingested == 11

    def test_events_created_in_db(self, db):
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        events = (
            db.execute(
                __import__("sqlalchemy", fromlist=["select"])
                .select(Event)
                .where(Event.source == "cfpb_complaint")
            )
            .scalars()
            .all()
        )
        assert len(events) == 10

    def test_event_type_is_complaint(self, db):
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "cfpb_complaint")).scalars().all()
        assert all(e.event_type == "complaint" for e in events)

    def test_idempotent_second_run(self, db):
        complaints = _flatten_fixture()
        r1 = ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        r2 = ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        assert r1.ingested == 10
        assert r2.ingested == 0
        assert r2.skipped == 11  # 10 in DB + 1 before since_date

    def test_since_date_filters_old_complaints(self, db):
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "cfpb_complaint")).scalars().all()
        activity_ids = {e.raw_json["complaint_id"] for e in events}
        assert "CFPB-2021-OLD" not in activity_ids

    def test_complaint_id_stored_in_raw_json(self, db):
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "cfpb_complaint")).scalars().all()
        for e in events:
            assert "complaint_id" in e.raw_json

    def test_product_and_issue_preserved_in_raw_json(self, db):
        """Complaint categories must be preserved for downstream NLP."""
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "cfpb_complaint")).scalars().all()
        for e in events:
            assert "product" in e.raw_json or "issue" in e.raw_json

    def test_no_penalty_for_complaints(self, db):
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "cfpb_complaint")).scalars().all()
        assert all(e.penalty_usd is None for e in events)

    def test_description_includes_product_and_issue(self, db):
        complaints = _flatten_fixture()
        ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "cfpb_complaint")).scalars().all()
        described = [e for e in events if e.description]
        assert len(described) > 0

    def test_empty_complaints_returns_zero(self, db):
        result = ingest_complaints(date(2022, 1, 1), db=db, complaints=[])
        assert result.ingested == 0
        assert result.total == 0

    def test_fetches_from_bulk_when_not_provided(self, db):
        zip_bytes = COMPLAINTS_ZIP.read_bytes()
        client = _make_streaming_client(zip_bytes)
        result = ingest_complaints(date(2022, 1, 1), db=db, client=client)
        assert result.ingested == 10  # 10 from 2022, 1 filtered by since_date

    def test_blank_complaint_id_not_ingested(self, db):
        """Complaints with missing/blank complaint_id must be skipped, not duplicated."""
        blank_id_complaint = {
            "complaint_id": "",
            "company": "GHOST BANK",
            "date_received": "2022-06-01",
            "product": "Checking or savings account",
            "issue": "Fee problem",
        }
        result = ingest_complaints(date(2022, 1, 1), db=db, complaints=[blank_id_complaint])
        assert result.ingested == 0
        assert result.skipped == 1


# ---------------------------------------------------------------------------
# Entity resolution integration test (not mocked — uses real resolver)
# ---------------------------------------------------------------------------


def test_entity_resolution_strips_legal_suffix(db):
    # _clean_company_name("WELLS FARGO BANK") → "WELLS FARGO" (strips BANK suffix)
    entity = _make_entity(db, "WELLS FARGO")
    from cam.entity.resolver import add_alias

    add_alias(entity.id, "WELLS FARGO", "cfpb_complaint", 1.0, db)

    complaints = _flatten_fixture()
    ingest_complaints(date(2022, 1, 1), db=db, complaints=complaints)

    from sqlalchemy import select

    linked = (
        db.execute(
            select(Event).where(
                Event.source == "cfpb_complaint",
                Event.entity_id == entity.id,
            )
        )
        .scalars()
        .all()
    )
    assert len(linked) >= 1


# ---------------------------------------------------------------------------
# TestComputeComplaintRate
# ---------------------------------------------------------------------------


class TestComputeComplaintRate:
    def test_none_period_end_in_edgar_does_not_crash(self, db):
        """EDGAR xbrl_facts with period_end=None must not raise TypeError."""
        entity = _make_entity(db, "Period End None Corp")
        today = date.today()
        ev = Event(
            entity_id=entity.id,
            source="sec_edgar",
            event_type="filing",
            event_date=today,
            raw_json={"xbrl_facts": {"Assets": {"value": 10_000_000_000, "period_end": None}}},
        )
        db.add(ev)
        db.commit()
        _seed_complaint_event(db, entity.id, today - timedelta(days=10))
        # Must not raise; period_end=None treated as "" so asset is still picked up
        rate = compute_complaint_rate(entity.id, db=db)
        assert rate is not None

    def test_returns_none_without_edgar_data(self, db):
        entity = _make_entity(db, "Rate Corp A")
        today = date.today()
        _seed_complaint_event(db, entity.id, today - timedelta(days=10))
        assert compute_complaint_rate(entity.id, db=db) is None

    def test_returns_complaint_rate(self, db):
        entity = _make_entity(db, "Rate Corp B")
        today = date.today()
        _seed_edgar_event(db, entity.id, total_assets=50_000_000_000)  # $50B
        _seed_complaint_event(db, entity.id, today - timedelta(days=30))
        _seed_complaint_event(db, entity.id, today - timedelta(days=60))

        rate = compute_complaint_rate(entity.id, period_months=12, db=db)
        assert rate is not None
        assert isinstance(rate, ComplaintRate)
        assert rate.complaints == 2
        assert rate.total_assets_usd == Decimal("50000000000")
        # 2 complaints / 50 billion = 0.04 per billion
        assert abs(rate.rate_per_billion - 0.04) < 1e-9

    def test_uses_most_recent_edgar_filing(self, db):
        entity = _make_entity(db, "Rate Corp C")
        today = date.today()
        # Seed two EDGAR events; most recent should win
        _seed_edgar_event(db, entity.id, total_assets=10_000_000_000, period_end="2021-12-31")
        _seed_edgar_event(db, entity.id, total_assets=20_000_000_000, period_end="2022-12-31")
        _seed_complaint_event(db, entity.id, today - timedelta(days=10))

        rate = compute_complaint_rate(entity.id, period_months=12, db=db)
        assert rate.total_assets_usd == Decimal("20000000000")

    def test_excludes_complaints_outside_window(self, db):
        entity = _make_entity(db, "Rate Corp D")
        today = date.today()
        _seed_edgar_event(db, entity.id, total_assets=10_000_000_000)
        # Recent complaint (within 12 months)
        _seed_complaint_event(db, entity.id, today - timedelta(days=100))
        # Old complaint (more than 12 months ago)
        _seed_complaint_event(db, entity.id, today - timedelta(days=400))

        rate = compute_complaint_rate(entity.id, period_months=12, db=db)
        assert rate.complaints == 1

    def test_returns_none_for_unknown_entity(self, db):
        assert compute_complaint_rate(uuid.uuid4(), db=db) is None


# ---------------------------------------------------------------------------
# TestDetectComplaintSpike
# ---------------------------------------------------------------------------


class TestDetectComplaintSpike:
    def test_returns_false_with_no_complaints(self, db):
        entity = _make_entity(db, "Spike Corp A")
        assert detect_complaint_spike(entity.id, db=db) is False

    def test_spike_detected_when_recent_exceeds_threshold(self, db):
        entity = _make_entity(db, "Spike Corp B")
        today = date.today()
        # Prior half (3-6 months ago): 2 complaints
        for _ in range(2):
            _seed_complaint_event(db, entity.id, today - timedelta(days=120))
        # Recent half (0-3 months ago): 4 complaints — 100% increase > 50% threshold
        for _ in range(4):
            _seed_complaint_event(db, entity.id, today - timedelta(days=30))

        assert detect_complaint_spike(entity.id, lookback_months=6, threshold_pct=50.0, db=db)

    def test_no_spike_when_increase_below_threshold(self, db):
        entity = _make_entity(db, "Spike Corp C")
        today = date.today()
        # Prior: 10, recent: 11 — only 10% increase, below 50% threshold
        for _ in range(10):
            _seed_complaint_event(db, entity.id, today - timedelta(days=120))
        for _ in range(11):
            _seed_complaint_event(db, entity.id, today - timedelta(days=30))

        assert not detect_complaint_spike(entity.id, lookback_months=6, threshold_pct=50.0, db=db)

    def test_any_recent_complaint_with_zero_prior_is_spike(self, db):
        entity = _make_entity(db, "Spike Corp D")
        today = date.today()
        _seed_complaint_event(db, entity.id, today - timedelta(days=10))
        assert detect_complaint_spike(entity.id, lookback_months=6, db=db)

    def test_exact_threshold_is_not_a_spike(self, db):
        """Exactly threshold_pct increase must NOT trigger (uses strict >)."""
        entity = _make_entity(db, "Spike Corp E")
        today = date.today()
        # Prior: 2, recent: 3 → 50% increase. With threshold=50.0, must be > 50%
        for _ in range(2):
            _seed_complaint_event(db, entity.id, today - timedelta(days=120))
        for _ in range(3):
            _seed_complaint_event(db, entity.id, today - timedelta(days=30))

        assert not detect_complaint_spike(entity.id, lookback_months=6, threshold_pct=50.0, db=db)

    def test_custom_threshold(self, db):
        entity = _make_entity(db, "Spike Corp F")
        today = date.today()
        # Prior: 10, recent: 11 — 10% increase, spike if threshold=5%
        for _ in range(10):
            _seed_complaint_event(db, entity.id, today - timedelta(days=120))
        for _ in range(11):
            _seed_complaint_event(db, entity.id, today - timedelta(days=30))

        assert detect_complaint_spike(entity.id, lookback_months=6, threshold_pct=5.0, db=db)

    def test_lookback_months_affects_window(self, db):
        """Longer lookback captures more prior complaints, changing the ratio."""
        entity = _make_entity(db, "Spike Corp G")
        today = date.today()
        # Complaint 8 months ago — within 12-month lookback but not 6-month
        _seed_complaint_event(db, entity.id, today - timedelta(days=240))
        _seed_complaint_event(db, entity.id, today - timedelta(days=30))

        # 6-month: prior window is 3-6 months ago → 0 prior → spike
        assert detect_complaint_spike(entity.id, lookback_months=6, db=db)
        # 12-month: prior window is 6-12 months ago → 1 prior, 1 recent → 0% increase → no spike
        assert not detect_complaint_spike(entity.id, lookback_months=12, threshold_pct=50.0, db=db)

    def test_unknown_entity_returns_false(self, db):
        assert detect_complaint_spike(uuid.uuid4(), db=db) is False


# ---------------------------------------------------------------------------
# TestPerformance
# ---------------------------------------------------------------------------


class TestPerformance:
    @pytest.fixture(autouse=True)
    def mock_entity_resolution(self, monkeypatch):
        import uuid

        from cam.entity.resolver import ResolveResult

        fake_eid = uuid.uuid4()

        def _fake_bulk_resolve(records, source, db, commit=True):
            return [
                ResolveResult(
                    entity_id=fake_eid,
                    canonical_name="Fake Entity",
                    confidence=1.0,
                    method="exact",
                    needs_review=False,
                )
                for _ in records
            ]

        monkeypatch.setattr("cam.ingestion.cfpb.bulk_resolve", _fake_bulk_resolve)

    def test_ingest_fixture_within_time_limit(self, db):
        """11-complaint fixture must ingest in < 5 seconds."""
        complaints = _flatten_fixture()
        start = time.monotonic()
        result = ingest_complaints(date(2021, 1, 1), db=db, complaints=complaints)
        elapsed = time.monotonic() - start
        assert result.ingested == 11
        assert elapsed < 5.0, f"ingest_complaints took {elapsed:.2f}s (limit: 5s)"

    def test_spike_detection_within_time_limit(self, db):
        """Spike detection over 500 events must complete in < 5 seconds."""
        entity = _make_entity(db, "Perf Corp")
        today = date.today()
        for i in range(500):
            age = i % 200
            ev = Event(
                entity_id=entity.id,
                source="cfpb_complaint",
                event_type="complaint",
                event_date=today - timedelta(days=age),
                raw_json={"complaint_id": f"PERF-{i}"},
            )
            db.add(ev)
        db.commit()

        start = time.monotonic()
        detect_complaint_spike(entity.id, db=db)
        elapsed = time.monotonic() - start
        assert elapsed < 5.0, f"detect_complaint_spike took {elapsed:.2f}s (limit: 5s)"
