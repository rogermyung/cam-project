"""
Unit tests for M3 — OSHA Ingestion (cam/ingestion/osha.py).

All external HTTP calls are mocked; no live network calls.
Uses SQLite in-memory DB for event persistence tests.
"""

from __future__ import annotations

import json
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, Entity, Event
from cam.ingestion.osha import (
    _clean_estab_name,
    _description,
    _event_type,
    _get_existing_activity_nrs,
    _is_retriable_error,
    _parse_date,
    _parse_penalty,
    download_bulk_data,
    fetch_recent_inspections,
    ingest_from_csv,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).parent.parent / "fixtures" / "osha"
SAMPLE_CSV = FIXTURES / "violations_sample.csv"
DOL_FIXTURE = FIXTURES / "dol_inspections_sample.json"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)


def _make_entity(db, canonical_name: str) -> Entity:
    e = Entity(canonical_name=canonical_name)
    db.add(e)
    db.commit()
    return e


def _make_response(data, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if isinstance(data, (dict, list)):
        resp.json.return_value = data
        resp.content = json.dumps(data).encode()
    else:
        resp.content = data if isinstance(data, bytes) else data.encode()
        resp.json.return_value = {}
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# TestCleanEstabName
# ---------------------------------------------------------------------------


class TestCleanEstabName:
    def test_strips_city_suffix(self):
        assert _clean_estab_name("AMAZON.COM SERVICES LLC - BALTIMORE") == "AMAZON.COM SERVICES LLC"

    def test_strips_two_word_suffix(self):
        assert _clean_estab_name("CONSTRUCTION CORP - DALLAS TX") == "CONSTRUCTION CORP"

    def test_no_suffix_unchanged(self):
        assert _clean_estab_name("ACME MANUFACTURING CO") == "ACME MANUFACTURING CO"

    def test_strips_whitespace(self):
        assert _clean_estab_name("  TYSON FOODS INC  ") == "TYSON FOODS INC"

    def test_empty_string(self):
        assert _clean_estab_name("") == ""

    def test_single_dash_not_stripped(self):
        # A dash without surrounding spaces is not a suffix separator
        result = _clean_estab_name("WALMART-SUPERCENTER")
        assert result == "WALMART-SUPERCENTER"

    def test_exxon_refinery_suffix(self):
        assert _clean_estab_name("EXXON MOBIL CORP - BAYTOWN REFINERY") == "EXXON MOBIL CORP"

    def test_smithfield_suffix(self):
        assert _clean_estab_name("SMITHFIELD FOODS INC - TAR HEEL") == "SMITHFIELD FOODS INC"


# ---------------------------------------------------------------------------
# TestParseDate
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_yyyymmdd_format(self):
        assert _parse_date("20220315") == date(2022, 3, 15)

    def test_iso_format(self):
        assert _parse_date("2022-03-15") == date(2022, 3, 15)

    def test_slash_format(self):
        assert _parse_date("03/15/2022") == date(2022, 3, 15)

    def test_empty_returns_none(self):
        assert _parse_date("") is None

    def test_whitespace_returns_none(self):
        assert _parse_date("   ") is None

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None

    def test_strips_whitespace(self):
        assert _parse_date("  20220315  ") == date(2022, 3, 15)


# ---------------------------------------------------------------------------
# TestParsePenalty
# ---------------------------------------------------------------------------


class TestParsePenalty:
    def test_plain_integer(self):
        assert _parse_penalty("12500") == Decimal("12500")

    def test_with_dollar_sign(self):
        assert _parse_penalty("$12500") == Decimal("12500")

    def test_with_comma(self):
        assert _parse_penalty("12,500") == Decimal("12500")

    def test_with_dollar_and_comma(self):
        assert _parse_penalty("$12,500.00") == Decimal("12500.00")

    def test_zero_returns_none(self):
        assert _parse_penalty("0") is None

    def test_empty_returns_none(self):
        assert _parse_penalty("") is None

    def test_whitespace_returns_none(self):
        assert _parse_penalty("   ") is None

    def test_invalid_returns_none(self):
        assert _parse_penalty("N/A") is None

    def test_decimal_string(self):
        assert _parse_penalty("12500.00") == Decimal("12500.00")


# ---------------------------------------------------------------------------
# TestEventType
# ---------------------------------------------------------------------------


class TestEventType:
    def test_with_violation_type_is_violation(self):
        assert _event_type({"violation_type": "S"}) == "violation"

    def test_willful_is_violation(self):
        assert _event_type({"violation_type": "W"}) == "violation"

    def test_empty_violation_type_is_inspection(self):
        assert _event_type({"violation_type": ""}) == "inspection"

    def test_missing_violation_type_is_inspection(self):
        assert _event_type({}) == "inspection"

    def test_whitespace_only_is_inspection(self):
        assert _event_type({"violation_type": "   "}) == "inspection"


# ---------------------------------------------------------------------------
# TestDescription
# ---------------------------------------------------------------------------


class TestDescription:
    def test_type_and_text(self):
        row = {"violation_type": "S", "citation_text": "Failure to guard machinery"}
        assert _description(row) == "S: Failure to guard machinery"

    def test_text_only(self):
        row = {"violation_type": "", "citation_text": "Some description"}
        assert _description(row) == "Some description"

    def test_type_only(self):
        row = {"violation_type": "W", "citation_text": ""}
        assert _description(row) == "W"

    def test_both_empty_returns_none(self):
        assert _description({"violation_type": "", "citation_text": ""}) is None

    def test_missing_keys_returns_none(self):
        assert _description({}) is None


# ---------------------------------------------------------------------------
# TestRetryLogic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_network_error_is_retriable(self):
        assert _is_retriable_error(httpx.NetworkError("connection reset"))

    def test_timeout_is_retriable(self):
        assert _is_retriable_error(httpx.TimeoutException("timed out"))

    def test_429_is_retriable(self):
        req = httpx.Request("GET", "https://example.com")
        resp = httpx.Response(429, request=req)
        assert _is_retriable_error(httpx.HTTPStatusError("429", request=req, response=resp))

    def test_500_is_not_retriable(self):
        req = httpx.Request("GET", "https://example.com")
        resp = httpx.Response(500, request=req)
        assert not _is_retriable_error(httpx.HTTPStatusError("500", request=req, response=resp))

    def test_value_error_not_retriable(self):
        assert not _is_retriable_error(ValueError("bad value"))


# ---------------------------------------------------------------------------
# TestDownloadBulkData
# ---------------------------------------------------------------------------


class TestDownloadBulkData:
    def test_downloads_and_saves_csv(self, tmp_path):
        csv_content = b"activity_nr,estab_name\n101001,ACME CO\n"
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(csv_content)

        with patch("cam.ingestion.osha.tempfile.gettempdir", return_value=str(tmp_path)):
            result_path = download_bulk_data(2022, client=client)

        assert result_path.exists()
        assert result_path.read_bytes() == csv_content

    def test_constructs_correct_url(self, tmp_path):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(b"a,b\n1,2\n")

        with patch("cam.ingestion.osha.tempfile.gettempdir", return_value=str(tmp_path)):
            download_bulk_data(2021, client=client)

        call_url = client.get.call_args[0][0]
        assert "2021" in call_url
        assert "osha.gov" in call_url

    def test_filename_includes_year(self, tmp_path):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(b"a,b\n")

        with patch("cam.ingestion.osha.tempfile.gettempdir", return_value=str(tmp_path)):
            path = download_bulk_data(2023, client=client)

        assert "2023" in path.name


# ---------------------------------------------------------------------------
# TestIngestFromCsv
# ---------------------------------------------------------------------------


class TestIngestFromCsv:
    def test_ingests_all_rows(self, db):
        result = ingest_from_csv(SAMPLE_CSV, db=db)
        assert result.total == 100
        assert result.ingested == 100
        assert result.skipped == 0
        assert result.errors == 0

    def test_events_created_in_db(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        count = db.execute(select(Event).where(Event.source == "osha")).scalars().all()
        assert len(count) == 100

    def test_idempotent_second_run_skips_all(self, db):
        r1 = ingest_from_csv(SAMPLE_CSV, db=db)
        r2 = ingest_from_csv(SAMPLE_CSV, db=db)
        assert r1.ingested == 100
        assert r2.ingested == 0
        assert r2.skipped == 100

    def test_since_date_filters_old_records(self, db):
        # All fixture records have open_date between 2021 and 2023
        result = ingest_from_csv(SAMPLE_CSV, since_date=date(2023, 1, 1), db=db)
        # Only 2023 records should be included
        assert result.ingested < 100
        assert result.ingested > 0

    def test_since_date_excludes_all_old(self, db):
        result = ingest_from_csv(SAMPLE_CSV, since_date=date(2030, 1, 1), db=db)
        assert result.ingested == 0

    def test_violation_event_type(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        violations = (
            db.execute(select(Event).where(Event.source == "osha", Event.event_type == "violation"))
            .scalars()
            .all()
        )
        assert len(violations) > 0

    def test_inspection_event_type(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        inspections = (
            db.execute(
                select(Event).where(Event.source == "osha", Event.event_type == "inspection")
            )
            .scalars()
            .all()
        )
        assert len(inspections) > 0

    def test_penalty_parsed_correctly(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        events_with_penalty = (
            db.execute(select(Event).where(Event.source == "osha", Event.penalty_usd.isnot(None)))
            .scalars()
            .all()
        )
        assert len(events_with_penalty) > 0
        for e in events_with_penalty:
            assert e.penalty_usd > 0

    def test_zero_penalty_stored_as_none(self, db):
        """Rows with initial_penalty=0 should store penalty_usd as NULL."""
        ingest_from_csv(SAMPLE_CSV, db=db)
        inspection_only = (
            db.execute(
                select(Event).where(
                    Event.source == "osha",
                    Event.event_type == "inspection",
                )
            )
            .scalars()
            .all()
        )
        for e in inspection_only:
            assert e.penalty_usd is None

    def test_event_date_populated(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        events = db.execute(select(Event).where(Event.source == "osha")).scalars().all()
        dated = [e for e in events if e.event_date is not None]
        assert len(dated) > 90  # nearly all rows have valid dates

    def test_raw_json_preserved(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        event = db.execute(select(Event).where(Event.source == "osha")).scalars().first()
        assert event.raw_json is not None
        assert "activity_nr" in event.raw_json
        assert "estab_name" in event.raw_json

    def test_activity_nr_in_raw_json(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        events = db.execute(select(Event).where(Event.source == "osha")).scalars().all()
        nrs = {e.raw_json["activity_nr"] for e in events}
        assert "101001" in nrs

    def test_missing_csv_path_returns_error(self, db):
        result = ingest_from_csv(Path("/nonexistent/path.csv"), db=db)
        assert result.errors == 1
        assert "Could not open CSV" in result.error_details[0]

    def test_description_built_from_type_and_text(self, db):
        ingest_from_csv(SAMPLE_CSV, db=db)
        events = (
            db.execute(select(Event).where(Event.source == "osha", Event.event_type == "violation"))
            .scalars()
            .all()
        )
        described = [e for e in events if e.description and ":" in e.description]
        assert len(described) > 0

    def test_entity_resolution_attempted(self, db):
        """Entity resolution runs; resolved entities link event to entity_id."""
        _make_entity(db, "ACME MANUFACTURING CO")
        # Re-seed alias so resolver can match
        from cam.entity.resolver import add_alias

        entity = db.execute(
            select(Entity).where(Entity.canonical_name == "ACME MANUFACTURING CO")
        ).scalar_one()
        add_alias(entity.id, "ACME MANUFACTURING CO", "osha", 1.0, db)

        ingest_from_csv(SAMPLE_CSV, db=db)

        linked = (
            db.execute(
                select(Event).where(
                    Event.source == "osha",
                    Event.entity_id == entity.id,
                )
            )
            .scalars()
            .all()
        )
        assert len(linked) > 0

    def test_estab_name_suffix_stripped_for_resolution(self, db):
        """Names like 'COMPANY - CITY' should resolve as 'COMPANY'."""
        _make_entity(db, "AMAZON.COM SERVICES LLC")
        from cam.entity.resolver import add_alias

        entity = db.execute(
            select(Entity).where(Entity.canonical_name == "AMAZON.COM SERVICES LLC")
        ).scalar_one()
        add_alias(entity.id, "AMAZON.COM SERVICES LLC", "osha", 1.0, db)

        ingest_from_csv(SAMPLE_CSV, db=db)

        # Rows with "AMAZON.COM SERVICES LLC - BALTIMORE" and "- SEATTLE"
        # should resolve to the same entity after suffix stripping
        linked = (
            db.execute(
                select(Event).where(
                    Event.source == "osha",
                    Event.entity_id == entity.id,
                )
            )
            .scalars()
            .all()
        )
        assert len(linked) >= 4  # at least 4 Amazon rows in fixture


# ---------------------------------------------------------------------------
# TestGetExistingActivityNrs
# ---------------------------------------------------------------------------


class TestGetExistingActivityNrs:
    def test_returns_empty_when_no_osha_events(self, db):
        assert _get_existing_activity_nrs(db) == set()

    def test_returns_activity_nrs_for_osha_events(self, db):
        event = Event(
            source="osha",
            event_type="violation",
            raw_json={"activity_nr": "999001"},
        )
        db.add(event)
        db.commit()
        nrs = _get_existing_activity_nrs(db)
        assert "999001" in nrs

    def test_excludes_other_sources(self, db):
        event = Event(
            source="sec_edgar",
            event_type="filing",
            raw_json={"activity_nr": "999002"},
        )
        db.add(event)
        db.commit()
        assert _get_existing_activity_nrs(db) == set()


# ---------------------------------------------------------------------------
# TestFetchRecentInspections
# ---------------------------------------------------------------------------


class TestFetchRecentInspections:
    def test_returns_list_of_dicts(self):
        fixture_data = json.loads(DOL_FIXTURE.read_text())
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(fixture_data)

        results = fetch_recent_inspections(30, client=client)

        assert isinstance(results, list)
        assert len(results) == 5

    def test_passes_date_range_params(self):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response([])

        fetch_recent_inspections(7, client=client)

        _, kwargs = client.get.call_args
        params = kwargs.get("params", {})
        assert "startDate" in params
        assert "endDate" in params

    def test_start_date_is_days_back_from_today(self):
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response([])

        today = date.today()
        fetch_recent_inspections(14, client=client)

        _, kwargs = client.get.call_args
        params = kwargs.get("params", {})
        from datetime import datetime

        start = datetime.fromisoformat(params["startDate"]).date()
        assert (today - start).days == 14

    def test_handles_wrapped_response(self):
        """DOL sometimes wraps the list under a 'data' key."""
        fixture_data = json.loads(DOL_FIXTURE.read_text())
        wrapped = {"data": fixture_data}
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(wrapped)

        results = fetch_recent_inspections(30, client=client)
        assert len(results) == 5

    def test_http_error_propagates(self):
        client = MagicMock(spec=httpx.Client)
        req = httpx.Request("GET", "https://example.com")
        resp = httpx.Response(500, request=req)
        client.get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=req, response=resp
        )

        with pytest.raises(httpx.HTTPStatusError):
            fetch_recent_inspections(30, client=client)


# ---------------------------------------------------------------------------
# TestPerformance
# ---------------------------------------------------------------------------


class TestPerformance:
    def test_ingest_csv_within_time_limit(self, db):
        """100-row fixture must ingest in < 5 seconds."""
        start = time.monotonic()
        result = ingest_from_csv(SAMPLE_CSV, db=db)
        elapsed = time.monotonic() - start

        assert result.ingested == 100
        assert elapsed < 5.0, f"ingest_from_csv took {elapsed:.2f}s (limit: 5s)"

    def test_parse_helpers_are_fast(self):
        """10 000 parse operations complete well under 1 second."""
        start = time.monotonic()
        for _ in range(10_000):
            _parse_date("20220315")
            _parse_penalty("$12,500.00")
            _clean_estab_name("AMAZON.COM SERVICES LLC - BALTIMORE")
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"parse helpers took {elapsed:.2f}s"
