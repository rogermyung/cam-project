"""
Unit tests for M4 — EPA Ingestion (cam/ingestion/epa.py).

All external HTTP calls are mocked; no live network calls.
Uses SQLite in-memory DB for event persistence tests.
"""

from __future__ import annotations

import io
import json
import math
import time
import uuid
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, Entity, Event
from cam.ingestion.epa import (
    _clean_facility_name,
    _get_existing_keys,
    _is_retriable_error,
    _parse_date,
    _parse_decimal,
    compute_tri_enforcement_divergence,
    ingest_echo_violations,
    ingest_tri,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "epa"
TRI_CSV = FIXTURES / "tri_sample.csv"
ECHO_JSON = FIXTURES / "echo_violations_sample.json"


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


def _seed_event(db, entity_id, source, event_type, event_date, penalty=None, raw_json=None):
    ev = Event(
        entity_id=entity_id,
        source=source,
        event_type=event_type,
        event_date=event_date,
        penalty_usd=penalty,
        raw_json=raw_json or {},
    )
    db.add(ev)
    db.commit()
    return ev


def _make_echo_zip_response(cases: list[dict], status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Response whose .content is a zip containing CASE_ENFORCEMENTS.csv.

    Column names are the uppercase bulk-export format the new _fetch_echo_cases
    implementation expects (ACTIVITY_ID, FAC_NAME, ACTIVITY_DATE, etc.).
    """
    fieldnames = [
        "ACTIVITY_ID",
        "FAC_NAME",
        "ACTIVITY_DATE",
        "PENALTY_ASSESSED_AMT",
        "ACTIVITY_TYPE_DESC",
        "CASE_NUMBER",
        "FRS_ID",
        "NAICS_CODE",
        "LAW_SECTION",
        "VIOLATION_TYPE",
    ]
    buf = io.StringIO()
    writer = __import__("csv").DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for c in cases:
        writer.writerow(
            {
                "ACTIVITY_ID": c.get("activity_id", ""),
                "FAC_NAME": c.get("facility_name", ""),
                "ACTIVITY_DATE": c.get("action_date", ""),
                "PENALTY_ASSESSED_AMT": c.get("penalty_assessed", ""),
                "ACTIVITY_TYPE_DESC": c.get("description", ""),
                "CASE_NUMBER": c.get("case_number", ""),
                "FRS_ID": c.get("frs_id", ""),
                "NAICS_CODE": c.get("naics_code", ""),
                "LAW_SECTION": c.get("law_section", ""),
                "VIOLATION_TYPE": c.get("violation_type", ""),
            }
        )
    csv_bytes = buf.getvalue().encode("utf-8")

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("CASE_ENFORCEMENTS.csv", csv_bytes)
    zip_bytes = zip_buf.getvalue()

    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = zip_bytes
    resp.raise_for_status.return_value = None
    return resp


def _make_http_error_response(status_code: int) -> MagicMock:
    """Build a mock response whose raise_for_status() raises HTTPStatusError."""
    req = httpx.Request("GET", "https://echo.epa.gov/files/echodownloads/case_downloads.zip")
    real_resp = httpx.Response(status_code, request=req)
    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = status_code
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        f"HTTP {status_code}", request=req, response=real_resp
    )
    return mock_resp


def _make_response(data, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if isinstance(data, (dict, list)):
        resp.json.return_value = data
        resp.content = json.dumps(data).encode()
    else:
        resp.content = data if isinstance(data, bytes) else (data or "").encode()
        resp.json.return_value = {}
    resp.raise_for_status.return_value = None
    return resp


def _load_echo_fixture() -> list[dict]:
    data = json.loads(ECHO_JSON.read_text())
    return data["Results"]["CaseList"]


# ---------------------------------------------------------------------------
# TestParseDecimal
# ---------------------------------------------------------------------------


class TestParseDecimal:
    def test_plain_integer(self):
        assert _parse_decimal("12500") == Decimal("12500")

    def test_with_comma(self):
        assert _parse_decimal("1,250,000") == Decimal("1250000")

    def test_decimal_string(self):
        assert _parse_decimal("875000.50") == Decimal("875000.50")

    def test_empty_returns_none(self):
        assert _parse_decimal("") is None

    def test_none_returns_none(self):
        assert _parse_decimal(None) is None

    def test_invalid_returns_none(self):
        assert _parse_decimal("N/A") is None

    def test_zero_returns_zero(self):
        assert _parse_decimal("0") == Decimal("0")

    def test_negative_returns_none(self):
        assert _parse_decimal("-500") is None


# ---------------------------------------------------------------------------
# TestParseDate
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_iso_format(self):
        assert _parse_date("2022-05-15") == date(2022, 5, 15)

    def test_slash_format(self):
        assert _parse_date("05/15/2022") == date(2022, 5, 15)

    def test_compact_format(self):
        assert _parse_date("20220515") == date(2022, 5, 15)

    def test_empty_returns_none(self):
        assert _parse_date("") is None

    def test_null_string_returns_none(self):
        assert _parse_date("null") is None

    def test_none_returns_none(self):
        assert _parse_date(None) is None

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# TestCleanFacilityName
# ---------------------------------------------------------------------------


class TestCleanFacilityName:
    def test_strips_location_suffix(self):
        assert (
            _clean_facility_name("EXXON MOBIL CORPORATION - BAYTOWN COMPLEX")
            == "EXXON MOBIL CORPORATION"
        )

    def test_no_suffix_unchanged(self):
        assert _clean_facility_name("DOW CHEMICAL COMPANY") == "DOW CHEMICAL COMPANY"

    def test_none_returns_empty(self):
        assert _clean_facility_name(None) == ""

    def test_strips_whitespace(self):
        assert _clean_facility_name("  3M COMPANY  ") == "3M COMPANY"

    def test_chevron_suffix(self):
        assert _clean_facility_name("CHEVRON USA INC - RICHMOND REFINERY") == "CHEVRON USA INC"


# ---------------------------------------------------------------------------
# TestRetryLogic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_network_error_retriable(self):
        assert _is_retriable_error(httpx.NetworkError("reset"))

    def test_timeout_retriable(self):
        assert _is_retriable_error(httpx.TimeoutException("timeout"))

    def test_429_retriable(self):
        req = httpx.Request("GET", "https://example.com")
        resp = httpx.Response(429, request=req)
        assert _is_retriable_error(httpx.HTTPStatusError("429", request=req, response=resp))

    def test_500_not_retriable(self):
        req = httpx.Request("GET", "https://example.com")
        resp = httpx.Response(500, request=req)
        assert not _is_retriable_error(httpx.HTTPStatusError("500", request=req, response=resp))

    def test_value_error_not_retriable(self):
        assert not _is_retriable_error(ValueError("bad"))


# ---------------------------------------------------------------------------
# TestGetExistingKeys
# ---------------------------------------------------------------------------


class TestGetExistingKeys:
    def test_empty_when_no_events(self, db):
        assert _get_existing_keys(db, "epa_tri", "tri_key") == set()

    def test_returns_keys_for_source(self, db):
        ev = Event(source="epa_tri", event_type="tri_release", raw_json={"tri_key": "K001"})
        db.add(ev)
        db.commit()
        assert "K001" in _get_existing_keys(db, "epa_tri", "tri_key")

    def test_excludes_other_sources(self, db):
        ev = Event(source="epa_echo", event_type="violation", raw_json={"tri_key": "K002"})
        db.add(ev)
        db.commit()
        assert _get_existing_keys(db, "epa_tri", "tri_key") == set()


# ---------------------------------------------------------------------------
# TestIngestTri
# ---------------------------------------------------------------------------


class TestIngestTri:
    def test_ingests_all_rows(self, db):
        # CSV has 21 rows total: 16 for year 2022, 5 for year 2021
        result = ingest_tri(2022, db=db, csv_path=TRI_CSV)
        assert result.total == 21
        assert result.ingested == 16
        assert result.skipped == 5
        assert result.errors == 0

    def test_year_filter_excludes_other_years(self, db):
        """Only rows whose YEAR matches the requested year are ingested."""
        result_2022 = ingest_tri(2022, db=db, csv_path=TRI_CSV)
        result_2021 = ingest_tri(2021, db=db, csv_path=TRI_CSV)
        assert result_2022.ingested == 16
        assert result_2021.ingested == 5

    def test_events_created_in_db(self, db):
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        assert len(events) == 16

    def test_event_type_is_tri_release(self, db):
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        assert all(e.event_type == "tri_release" for e in events)

    def test_idempotent_second_run(self, db):
        r1 = ingest_tri(2022, db=db, csv_path=TRI_CSV)
        r2 = ingest_tri(2022, db=db, csv_path=TRI_CSV)
        assert r1.ingested == 16
        assert r2.ingested == 0
        assert r2.skipped == 21  # 16 already in DB + 5 wrong year

    def test_event_date_is_dec_31_of_year(self, db):
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        for e in events:
            if e.event_date:
                assert e.event_date.month == 12
                assert e.event_date.day == 31

    def test_description_includes_chemical(self, db):
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        described = [e for e in events if e.description]
        assert all("release" in d.description.lower() for d in described)

    def test_raw_json_preserved(self, db):
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        event = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().first()
        assert event.raw_json is not None
        assert "tri_key" in event.raw_json
        assert "FACILITY_NAME" in event.raw_json

    def test_tri_key_in_raw_json(self, db):
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        for e in events:
            assert "tri_key" in e.raw_json
            assert "|" in e.raw_json["tri_key"]  # FRS_ID|CHEMICAL|YEAR format

    def test_no_penalty_for_tri(self, db):
        """TRI releases are self-reported; there is no penalty_usd."""
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        assert all(e.penalty_usd is None for e in events)

    def test_missing_csv_returns_error(self, db):
        result = ingest_tri(2022, db=db, csv_path=Path("/nonexistent/tri.csv"))
        assert result.errors == 1
        assert "Could not open TRI CSV" in result.error_details[0]

    def test_downloads_csv_when_path_not_provided(self, db, tmp_path):
        csv_bytes = TRI_CSV.read_bytes()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(csv_bytes)

        with patch("cam.ingestion.epa.tempfile.gettempdir", return_value=str(tmp_path)):
            result = ingest_tri(2022, db=db, client=client)

        assert result.ingested == 16  # 16 of 21 rows are for year 2022
        assert "2022" in client.get.call_args[0][0]

    def test_grams_normalized_to_pounds(self, db):
        """Rows with UNIT_OF_MEASURE=Grams must have total_releases_lbs stored in raw_json."""
        ingest_tri(2022, db=db, csv_path=TRI_CSV)
        from decimal import Decimal

        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_tri")).scalars().all()
        # International Paper dioxin row uses Grams; 0.082 grams * 0.00220462 ≈ 0.000181 lbs
        grams_events = [e for e in events if (e.raw_json or {}).get("UNIT_OF_MEASURE") == "Grams"]
        assert grams_events, "Expected at least one Grams row"
        for ev in grams_events:
            raw_releases = Decimal((ev.raw_json or {}).get("TOTAL_RELEASES", "0"))
            lbs_stored = Decimal((ev.raw_json or {}).get("total_releases_lbs", "0"))
            expected_lbs = raw_releases * Decimal("0.00220462")
            assert abs(lbs_stored - expected_lbs) < Decimal("1e-10")

    def test_entity_resolution_uses_parent_company(self, db):
        """Parent company name is preferred over facility name for resolution."""
        entity = _make_entity(db, "EXXON MOBIL CORP")
        from cam.entity.resolver import add_alias

        add_alias(entity.id, "EXXON MOBIL CORP", "epa_tri", 1.0, db)

        ingest_tri(2022, db=db, csv_path=TRI_CSV)

        from sqlalchemy import select

        linked = (
            db.execute(select(Event).where(Event.source == "epa_tri", Event.entity_id == entity.id))
            .scalars()
            .all()
        )
        assert len(linked) > 0


# ---------------------------------------------------------------------------
# TestIngestEchoViolations
# ---------------------------------------------------------------------------


class TestIngestEchoViolations:
    def test_ingests_all_cases(self, db):
        cases = _load_echo_fixture()
        result = ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        assert result.total == 8
        assert result.ingested == 8
        assert result.errors == 0

    def test_events_created_in_db(self, db):
        cases = _load_echo_fixture()
        ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_echo")).scalars().all()
        assert len(events) == 8

    def test_event_type_is_violation(self, db):
        cases = _load_echo_fixture()
        ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_echo")).scalars().all()
        assert all(e.event_type == "violation" for e in events)

    def test_idempotent_second_run(self, db):
        cases = _load_echo_fixture()
        r1 = ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        r2 = ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        assert r1.ingested == 8
        assert r2.ingested == 0
        assert r2.skipped == 8

    def test_penalty_parsed_correctly(self, db):
        cases = _load_echo_fixture()
        ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        from sqlalchemy import select

        events_with_penalty = (
            db.execute(
                select(Event).where(Event.source == "epa_echo", Event.penalty_usd.isnot(None))
            )
            .scalars()
            .all()
        )
        # All cases except the unsettled Ford case have penalties
        assert len(events_with_penalty) >= 7
        for e in events_with_penalty:
            assert e.penalty_usd > 0

    def test_null_settlement_date_handled(self, db):
        """Cases with null settlement_date must not error."""
        cases = _load_echo_fixture()
        result = ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        assert result.errors == 0

    def test_event_date_populated(self, db):
        cases = _load_echo_fixture()
        ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_echo")).scalars().all()
        assert all(e.event_date is not None for e in events)

    def test_activity_id_in_raw_json(self, db):
        cases = _load_echo_fixture()
        ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_echo")).scalars().all()
        for e in events:
            assert "activity_id" in e.raw_json

    def test_empty_cases_returns_zero(self, db):
        result = ingest_echo_violations(date(2021, 1, 1), db=db, cases=[])
        assert result.ingested == 0
        assert result.total == 0

    def test_since_date_filters_old_cases(self, db):
        """Cases with action_date before since_date must be excluded."""
        cases = _load_echo_fixture()
        # ECHO-CAA-003 has action_date 2021-07-19 → excluded when since_date=2022-01-01
        result = ingest_echo_violations(date(2022, 1, 1), db=db, cases=cases)
        assert result.ingested == 7
        assert result.skipped == 1
        from sqlalchemy import select

        events = db.execute(select(Event).where(Event.source == "epa_echo")).scalars().all()
        activity_ids = {e.raw_json["activity_id"] for e in events}
        assert "ECHO-CAA-003" not in activity_ids

    def test_fetches_from_bulk_zip_when_cases_not_provided(self, db):
        """_fetch_echo_cases downloads the bulk zip and filters by since_date."""
        cases = _load_echo_fixture()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_echo_zip_response(cases)

        # since_date=2022-01-01 excludes ECHO-CAA-003 (action_date 2021-07-19)
        result = ingest_echo_violations(date(2022, 1, 1), db=db, client=client)
        assert result.ingested == 7

    def test_bad_zip_returns_zero(self, db):
        """Corrupt zip content is handled gracefully — returns 0 ingested."""
        client = MagicMock(spec=httpx.Client)
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.content = b"not a zip file"
        resp.raise_for_status.return_value = None
        client.get.return_value = resp

        result = ingest_echo_violations(date(2022, 1, 1), db=db, client=client)
        assert result.ingested == 0

    def test_http_404_returns_empty(self, db):
        """A 404 from the ECHO bulk zip endpoint is treated as 'not published yet'."""
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_http_error_response(404)

        result = ingest_echo_violations(date(2022, 1, 1), db=db, client=client)
        assert result.ingested == 0
        assert result.total == 0

    def test_http_410_returns_empty(self, db):
        """A 410 (Gone) is also treated as a benign 'not available' condition."""
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_http_error_response(410)

        result = ingest_echo_violations(date(2022, 1, 1), db=db, client=client)
        assert result.ingested == 0

    def test_http_500_raises(self, db):
        """A 500 from the ECHO endpoint must propagate — not silently return []."""
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_http_error_response(500)

        with pytest.raises(httpx.HTTPStatusError):
            ingest_echo_violations(date(2022, 1, 1), db=db, client=client)

    def test_http_403_raises(self, db):
        """A 403 (e.g. WAF block) must propagate as a hard failure."""
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_http_error_response(403)

        with pytest.raises(httpx.HTTPStatusError):
            ingest_echo_violations(date(2022, 1, 1), db=db, client=client)

    def test_unexpected_api_response_returns_zero(self, db):
        client = MagicMock(spec=httpx.Client)
        mock_resp = _make_response([])
        mock_resp.json.return_value = "unexpected"
        client.get.return_value = mock_resp

        result = ingest_echo_violations(date(2022, 1, 1), db=db, client=client)
        assert result.ingested == 0

    def test_entity_resolution_strips_facility_suffix(self, db):
        entity = _make_entity(db, "EXXON MOBIL CORPORATION")
        from cam.entity.resolver import add_alias

        add_alias(entity.id, "EXXON MOBIL CORPORATION", "epa_echo", 1.0, db)

        cases = _load_echo_fixture()
        ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)

        from sqlalchemy import select

        linked = (
            db.execute(
                select(Event).where(Event.source == "epa_echo", Event.entity_id == entity.id)
            )
            .scalars()
            .all()
        )
        assert len(linked) >= 1


# ---------------------------------------------------------------------------
# TestComputeDivergence
# ---------------------------------------------------------------------------


class TestComputeDivergence:
    def _seed_tri(self, db, entity_id, year, total_releases: float):
        return _seed_event(
            db,
            entity_id,
            "epa_tri",
            "tri_release",
            date(year, 12, 31),
            raw_json={
                "TOTAL_RELEASES": str(total_releases),
                "tri_key": f"{entity_id}|Benzene|{year}",
            },
        )

    def _seed_echo(self, db, entity_id, year, penalty: float):
        return _seed_event(
            db,
            entity_id,
            "epa_echo",
            "violation",
            date(year, 6, 1),
            penalty=Decimal(str(penalty)),
            raw_json={"activity_id": f"ECHO-{entity_id}-{year}"},
        )

    def test_returns_none_without_tri_data(self, db):
        entity = _make_entity(db, "Test Corp A")
        self._seed_echo(db, entity.id, 2022, 500000)
        assert compute_tri_enforcement_divergence(entity.id, 2022, db=db) is None

    def test_returns_none_without_echo_data(self, db):
        entity = _make_entity(db, "Test Corp B")
        self._seed_tri(db, entity.id, 2022, 10000)
        assert compute_tri_enforcement_divergence(entity.id, 2022, db=db) is None

    def test_returns_float_with_both_datasets(self, db):
        entity = _make_entity(db, "Test Corp C")
        self._seed_tri(db, entity.id, 2022, 10000)
        self._seed_echo(db, entity.id, 2022, 500000)
        score = compute_tri_enforcement_divergence(entity.id, 2022, db=db)
        assert score is not None
        assert isinstance(score, float)
        assert score > 0

    def test_higher_penalty_relative_to_releases_gives_higher_score(self, db):
        """High penalty / low releases → higher divergence than low penalty / high releases."""
        entity_hi = _make_entity(db, "High Divergence Corp")
        entity_lo = _make_entity(db, "Low Divergence Corp")

        # High: large penalty, small reported releases
        self._seed_tri(db, entity_hi.id, 2022, 100)
        self._seed_echo(db, entity_hi.id, 2022, 5_000_000)

        # Low: small penalty, large reported releases
        self._seed_tri(db, entity_lo.id, 2022, 500_000)
        self._seed_echo(db, entity_lo.id, 2022, 10_000)

        hi_score = compute_tri_enforcement_divergence(entity_hi.id, 2022, db=db)
        lo_score = compute_tri_enforcement_divergence(entity_lo.id, 2022, db=db)

        assert hi_score > lo_score

    def test_year_filter_applied(self, db):
        """TRI and ECHO events from different years must not be mixed."""
        entity = _make_entity(db, "Year Filter Corp")
        self._seed_tri(db, entity.id, 2021, 10000)  # different year
        self._seed_echo(db, entity.id, 2022, 500000)  # different year
        # No matching year → None
        assert compute_tri_enforcement_divergence(entity.id, 2022, db=db) is None

    def test_multiple_tri_events_summed(self, db):
        """Total releases summed across multiple chemicals."""
        entity = _make_entity(db, "Multi Chemical Corp")
        self._seed_tri(db, entity.id, 2022, 1000)
        self._seed_tri(db, entity.id, 2022, 2000)
        self._seed_echo(db, entity.id, 2022, 300000)

        score_multi = compute_tri_enforcement_divergence(entity.id, 2022, db=db)

        entity2 = _make_entity(db, "Single Chemical Corp")
        self._seed_tri(db, entity2.id, 2022, 3000)
        self._seed_echo(db, entity2.id, 2022, 300000)

        score_single = compute_tri_enforcement_divergence(entity2.id, 2022, db=db)

        # Same total releases → same score
        assert abs(score_multi - score_single) < 1e-6

    def test_uses_log1p_formula(self, db):
        """Verify the exact formula: log1p(penalty / (releases + 1))."""
        entity = _make_entity(db, "Formula Test Corp")
        releases = 5000.0
        penalty = 1_000_000.0
        self._seed_tri(db, entity.id, 2022, releases)
        self._seed_echo(db, entity.id, 2022, penalty)

        score = compute_tri_enforcement_divergence(entity.id, 2022, db=db)
        expected = math.log1p(penalty / (releases + 1))
        assert abs(score - expected) < 1e-9

    def test_unknown_entity_returns_none(self, db):
        unknown_id = uuid.uuid4()
        assert compute_tri_enforcement_divergence(unknown_id, 2022, db=db) is None


# ---------------------------------------------------------------------------
# TestPerformance
# ---------------------------------------------------------------------------


class TestPerformance:
    def test_ingest_tri_fixture_within_time_limit(self, db):
        """TRI fixture must ingest in < 5 seconds."""
        start = time.monotonic()
        result = ingest_tri(2022, db=db, csv_path=TRI_CSV)
        elapsed = time.monotonic() - start
        assert result.ingested == 16  # 16 of 21 rows are for year 2022
        assert elapsed < 5.0, f"ingest_tri took {elapsed:.2f}s (limit: 5s)"

    def test_ingest_echo_fixture_within_time_limit(self, db):
        """8-case ECHO fixture must ingest in < 5 seconds."""
        cases = _load_echo_fixture()
        start = time.monotonic()
        result = ingest_echo_violations(date(2021, 1, 1), db=db, cases=cases)
        elapsed = time.monotonic() - start
        assert result.ingested == 8
        assert elapsed < 5.0, f"ingest_echo_violations took {elapsed:.2f}s (limit: 5s)"
