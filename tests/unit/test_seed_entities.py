"""
Unit tests for cam/entity/seed.py — seeding Entity + EntityAlias rows
from SEC EDGAR's company_tickers.json.

All external HTTP calls are mocked via unittest.mock (no live network calls).
Uses SQLite in-memory DB (no live Postgres required).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, Entity, EntityAlias
from cam.entity.seed import (
    DEFAULT_BATCH_SIZE,
    SEC_TICKERS_URL,
    _upsert_batch,
    fetch_tickers,
    seed,
)

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).parent.parent / "fixtures" / "edgar"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _make_httpx_response(data: dict, status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Response that returns *data* as JSON."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# DB fixtures — SQLite in-memory, module-scoped engine, function-scoped session
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    """In-memory SQLite engine for fast, DB-free tests.

    Function-scoped so each test gets a completely isolated database
    (seed() calls db.commit() internally, so a module-scoped engine
    would leak committed rows across tests).
    """
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db(engine):
    """Provide a session backed by a fresh in-memory database."""
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Sample ticker data
# ---------------------------------------------------------------------------

SAMPLE_TICKERS = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
    "2": {"cik_str": 1018724, "ticker": "AMZN", "title": "Amazon.com Inc."},
}


# ---------------------------------------------------------------------------
# fetch_tickers tests
# ---------------------------------------------------------------------------


class TestFetchTickers:
    def test_returns_parsed_json(self):
        """fetch_tickers returns the parsed JSON dict from SEC."""
        mock_resp = _make_httpx_response(SAMPLE_TICKERS)
        with patch("httpx.get", return_value=mock_resp) as mock_get:
            result = fetch_tickers("test@example.com")

        mock_get.assert_called_once_with(
            SEC_TICKERS_URL,
            headers={"User-Agent": "test@example.com"},
            timeout=30,
            follow_redirects=True,
        )
        assert result == SAMPLE_TICKERS

    def test_raises_on_http_error(self):
        """fetch_tickers propagates HTTP errors from raise_for_status."""
        mock_resp = _make_httpx_response({}, status_code=500)
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error",
            request=MagicMock(),
            response=mock_resp,
        )
        with patch("httpx.get", return_value=mock_resp):
            with pytest.raises(httpx.HTTPStatusError):
                fetch_tickers("test@example.com")

    def test_uses_fixture_schema(self):
        """The company_tickers.json fixture matches the real SEC schema."""
        data = _load_fixture("company_tickers.json")
        assert len(data) >= 2
        for entry in data.values():
            assert "cik_str" in entry
            assert "ticker" in entry
            assert "title" in entry


# ---------------------------------------------------------------------------
# seed() core logic tests
# ---------------------------------------------------------------------------


class TestSeed:
    def test_inserts_new_entities(self, db):
        """seed() creates Entity rows for each company in tickers."""
        inserted, skipped = seed(db, SAMPLE_TICKERS)

        assert inserted == 3
        assert skipped == 0

        entities = db.execute(select(Entity)).scalars().all()
        tickers_in_db = {e.ticker for e in entities}
        assert tickers_in_db == {"AAPL", "MSFT", "AMZN"}

    def test_inserts_aliases(self, db):
        """seed() creates one EntityAlias per Entity with source='sec_seed'."""
        seed(db, SAMPLE_TICKERS)

        aliases = db.execute(select(EntityAlias)).scalars().all()
        assert len(aliases) == 3
        for alias in aliases:
            assert alias.source == "sec_seed"
            assert alias.confidence == 1.0

    def test_alias_names_match_canonical(self, db):
        """Each alias raw_name matches the entity's canonical_name."""
        seed(db, SAMPLE_TICKERS)

        entities = {e.id: e.canonical_name for e in db.execute(select(Entity)).scalars().all()}
        aliases = db.execute(select(EntityAlias)).scalars().all()
        for alias in aliases:
            assert alias.raw_name == entities[alias.entity_id]

    def test_idempotent_on_second_call(self, db):
        """Running seed() twice produces the same DB state (no duplicates)."""
        inserted1, skipped1 = seed(db, SAMPLE_TICKERS)
        inserted2, skipped2 = seed(db, SAMPLE_TICKERS)

        assert inserted1 == 3
        assert inserted2 == 0          # all already exist
        assert skipped2 == 3

        entity_count = db.execute(text("SELECT COUNT(*) FROM entities")).scalar()
        alias_count = db.execute(text("SELECT COUNT(*) FROM entity_aliases")).scalar()
        assert entity_count == 3
        assert alias_count == 3

    def test_skips_blank_ticker(self, db):
        """Entries with an empty ticker are counted as skipped."""
        tickers = {
            "0": {"cik_str": 1, "ticker": "", "title": "No Ticker Corp"},
            "1": {"cik_str": 2, "ticker": "GOOD", "title": "Good Corp"},
        }
        inserted, skipped = seed(db, tickers)
        assert inserted == 1
        assert skipped == 1

    def test_skips_blank_title(self, db):
        """Entries with an empty title are counted as skipped."""
        tickers = {
            "0": {"cik_str": 1, "ticker": "NT", "title": ""},
            "1": {"cik_str": 2, "ticker": "GOOD", "title": "Good Corp"},
        }
        inserted, skipped = seed(db, tickers)
        assert inserted == 1
        assert skipped == 1

    def test_dry_run_writes_nothing(self, db):
        """dry_run=True counts insertions without touching the database."""
        inserted, skipped = seed(db, SAMPLE_TICKERS, dry_run=True)

        assert inserted == 3
        assert skipped == 0

        entity_count = db.execute(text("SELECT COUNT(*) FROM entities")).scalar()
        assert entity_count == 0

    def test_batch_size_respected(self, db):
        """seed() commits in batches; small batch_size still inserts all rows."""
        # With batch_size=1, each entity triggers a separate commit
        inserted, skipped = seed(db, SAMPLE_TICKERS, batch_size=1)
        assert inserted == 3

        entity_count = db.execute(text("SELECT COUNT(*) FROM entities")).scalar()
        assert entity_count == 3

    def test_uses_fixture_data(self, db):
        """Smoke-test with the real fixture file — all entries are seeded."""
        fixture_data = _load_fixture("company_tickers.json")
        inserted, skipped = seed(db, fixture_data)
        assert inserted == len(fixture_data)
        assert skipped == 0

    def test_ticker_uppercased(self, db):
        """Tickers are normalised to uppercase regardless of source case."""
        tickers = {"0": {"cik_str": 1, "ticker": "aapl", "title": "Apple Inc."}}
        seed(db, tickers)
        entity = db.execute(select(Entity).where(Entity.ticker == "AAPL")).scalars().first()
        assert entity is not None

    def test_returns_counts(self, db):
        """seed() returns (inserted, skipped) as a 2-tuple of ints."""
        result = seed(db, SAMPLE_TICKERS)
        assert isinstance(result, tuple)
        assert len(result) == 2
        inserted, skipped = result
        assert isinstance(inserted, int)
        assert isinstance(skipped, int)


# ---------------------------------------------------------------------------
# Integration-style: fetch_tickers + seed round-trip (HTTP mocked)
# ---------------------------------------------------------------------------


class TestFetchAndSeedRoundTrip:
    def test_round_trip_with_mocked_http(self, db):
        """Mocked HTTP fetch → seed: verifies the full data flow end-to-end."""
        mock_resp = _make_httpx_response(SAMPLE_TICKERS)
        with patch("httpx.get", return_value=mock_resp):
            tickers = fetch_tickers("agent@cam-project.org")

        inserted, skipped = seed(db, tickers)
        assert inserted == 3
        assert skipped == 0

        # Verify each company is in the DB
        for entry in SAMPLE_TICKERS.values():
            entity = (
                db.execute(select(Entity).where(Entity.ticker == entry["ticker"]))
                .scalars()
                .first()
            )
            assert entity is not None, f"Expected ticker {entry['ticker']} in DB"
            assert entity.canonical_name == entry["title"]
