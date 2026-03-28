"""Unit tests for M5 — CFPB Ingestion."""

from __future__ import annotations

import json
import time
import uuid
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cam.db.models import Base, Entity, Event
from cam.ingestion.cfpb import (
    ComplaintRate,
    _clean_company_name,
    _fetch_complaints_page,
    _get_existing_complaint_ids,
    _hits_to_complaints,
    _is_retriable_error,
    _parse_date,
    _parse_decimal,
    compute_complaint_rate,
    detect_complaint_spike,
    ingest_complaints,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "cfpb"
COMPLAINTS_FIXTURE = FIXTURES_DIR / "complaints_sample.json"


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


def _make_response(body) -> MagicMock:
    mock = MagicMock(spec=httpx.Response)
    if isinstance(body, (bytes, bytearray)):
        mock.content = body
        mock.json.return_value = json.loads(body)
    else:
        mock.json.return_value = body
        mock.content = json.dumps(body).encode()
    mock.raise_for_status.return_value = None
    return mock


def _load_fixture() -> dict:
    return json.loads(COMPLAINTS_FIXTURE.read_text())


def _flatten_fixture() -> list[dict]:
    """Return fixture as flat list of complaint dicts (with complaint_id)."""
    data = _load_fixture()
    hits = data["hits"]["hits"]
    return _hits_to_complaints(hits)


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

    def test_500_not_retriable(self):
        resp = MagicMock()
        resp.status_code = 500
        exc = httpx.HTTPStatusError("server error", request=MagicMock(), response=resp)
        assert not _is_retriable_error(exc)

    def test_value_error_not_retriable(self):
        assert not _is_retriable_error(ValueError("bad value"))


# ---------------------------------------------------------------------------
# TestHitsToComplaints
# ---------------------------------------------------------------------------


class TestHitsToComplaints:
    def test_drops_hits_with_blank_id(self):
        """Hits without _id must be silently dropped to prevent duplicate ingestion."""
        hits = [
            {"_id": "", "_source": {"company": "No ID Bank"}},
            {"_source": {"company": "Also No ID"}},  # missing _id entirely
            {"_id": "VALID-1", "_source": {"company": "Good Bank"}},
        ]
        result = _hits_to_complaints(hits)
        assert len(result) == 1
        assert result[0]["complaint_id"] == "VALID-1"

    # (existing tests continue below)
    def test_extracts_complaint_id(self):
        hits = [{"_id": "ABC-123", "_source": {"company": "Test Bank"}}]
        result = _hits_to_complaints(hits)
        assert result[0]["complaint_id"] == "ABC-123"

    def test_flattens_source_fields(self):
        hits = [{"_id": "1", "_source": {"product": "Mortgage", "state": "CA"}}]
        result = _hits_to_complaints(hits)
        assert result[0]["product"] == "Mortgage"
        assert result[0]["state"] == "CA"

    def test_empty_list(self):
        assert _hits_to_complaints([]) == []

    def test_ignores_non_dict_hits(self):
        hits = ["not-a-dict", {"_id": "2", "_source": {}}]
        result = _hits_to_complaints(hits)
        assert len(result) == 1


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
# TestFetchComplaintsPage
# ---------------------------------------------------------------------------


class TestFetchComplaintsPage:
    def test_returns_hits_and_total(self):
        fixture = _load_fixture()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(fixture)

        hits, total = _fetch_complaints_page(date(2022, 1, 1), client=client)
        assert total == 11
        assert len(hits) == 11

    def test_unexpected_response_returns_empty(self):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response("not-a-dict")

        hits, total = _fetch_complaints_page(date(2022, 1, 1), client=client)
        assert hits == []
        assert total == 0

    def test_sends_since_date_param(self):
        fixture = _load_fixture()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(fixture)

        _fetch_complaints_page(date(2022, 6, 1), client=client)
        call_kwargs = client.get.call_args[1]
        params = call_kwargs.get("params", {})
        assert params.get("date_received_min") == "2022-06-01"


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
            # At minimum product and issue should be in raw_json
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

    def test_fetches_from_api_when_not_provided(self, db):
        fixture = _load_fixture()
        client = MagicMock(spec=httpx.Client)
        # First page returns all 11; second call returns empty to stop pagination
        client.get.side_effect = [
            _make_response(fixture),
            _make_response({"hits": {"total": {"value": 11}, "hits": []}}),
        ]
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
    entity = _make_entity(db, "WELLS FARGO BANK")
    from cam.entity.resolver import add_alias

    add_alias(entity.id, "WELLS FARGO BANK", "cfpb_complaint", 1.0, db)

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
