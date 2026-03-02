"""
Unit tests for M2 — EDGAR Ingestion (cam/ingestion/edgar.py).

All external HTTP calls are mocked via unittest.mock; no live network calls.
All S3 calls are mocked; no live object store required.
Uses SQLite in-memory DB for event persistence tests.
"""

from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, Entity, Event
from cam.ingestion.edgar import (
    FilingDocument,
    FilingMetadata,
    IngestResult,
    _accession_no_dashes,
    _filing_in_db,
    _filing_url,
    _object_exists,
    _object_store_key,
    _upsert_filing_event,
    download_filing,
    fetch_company_filings,
    get_cik_for_ticker,
    ingest_all_10k,
)

# ---------------------------------------------------------------------------
# Fixtures directory helpers
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).parent.parent / "fixtures" / "edgar"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _make_response(data: dict | str, status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if isinstance(data, dict):
        resp.json.return_value = data
        resp.text = json.dumps(data)
    else:
        resp.text = data
        resp.json.side_effect = ValueError("not json")
    resp.raise_for_status.return_value = None
    return resp


def _make_429_response() -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 429
    req = MagicMock(spec=httpx.Request)
    resp.request = req
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "429", request=req, response=resp
    )
    return resp


# ---------------------------------------------------------------------------
# SQLite in-memory database fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture()
def sample_entity(db):
    entity = Entity(
        id=uuid.uuid4(),
        canonical_name="Apple Inc.",
        ticker="AAPL",
    )
    db.add(entity)
    db.commit()
    return entity


# ---------------------------------------------------------------------------
# Helper data
# ---------------------------------------------------------------------------

APPLE_CIK = "0000320193"
APPLE_TICKER = "AAPL"
APPLE_ACCESSION = "0000320193-24-000006"
APPLE_FILED = date(2024, 2, 2)
APPLE_DOC = "aapl-20231230.htm"


def _apple_filing() -> FilingMetadata:
    return FilingMetadata(
        cik=APPLE_CIK,
        accession_number=APPLE_ACCESSION,
        filing_type="10-K",
        filed_date=APPLE_FILED,
        primary_document=APPLE_DOC,
    )


# ===========================================================================
# get_cik_for_ticker
# ===========================================================================


class TestGetCikForTicker:
    def test_resolves_known_ticker(self):
        tickers_data = _load_fixture("company_tickers.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        cik = get_cik_for_ticker("AAPL", client=client)

        assert cik == "0000320193"
        assert len(cik) == 10  # zero-padded

    def test_resolves_ticker_case_insensitive(self):
        tickers_data = _load_fixture("company_tickers.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        cik = get_cik_for_ticker("aapl", client=client)

        assert cik == "0000320193"

    def test_resolves_microsoft(self):
        tickers_data = _load_fixture("company_tickers.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        cik = get_cik_for_ticker("MSFT", client=client)

        assert cik == "0000789019"

    def test_resolves_amazon(self):
        tickers_data = _load_fixture("company_tickers.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        cik = get_cik_for_ticker("AMZN", client=client)

        assert cik == "0001018724"

    def test_returns_none_for_unknown_ticker(self):
        tickers_data = _load_fixture("company_tickers.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        cik = get_cik_for_ticker("NOTREAL", client=client)

        assert cik is None

    def test_returns_none_on_http_error(self):
        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = httpx.NetworkError("connection refused")

        cik = get_cik_for_ticker("AAPL", client=client)

        assert cik is None

    def test_returns_none_on_invalid_json(self):
        client = MagicMock(spec=httpx.Client)
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.side_effect = ValueError("bad json")
        client.get.return_value = resp

        cik = get_cik_for_ticker("AAPL", client=client)

        assert cik is None

    def test_cik_is_zero_padded_to_10_digits(self):
        """CIK with fewer than 10 digits must be zero-padded."""
        tickers_data = {"0": {"cik_str": 12345, "ticker": "TINY", "title": "Tiny Corp"}}
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        cik = get_cik_for_ticker("TINY", client=client)

        assert cik == "0000012345"
        assert len(cik) == 10


# ===========================================================================
# fetch_company_filings
# ===========================================================================


class TestFetchCompanyFilings:
    def test_returns_10k_filings_for_apple(self):
        apple_data = _load_fixture("submissions_apple.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(apple_data)

        filings = fetch_company_filings(
            APPLE_CIK, ["10-K"], date(2020, 1, 1), client=client
        )

        assert len(filings) == 4  # four 10-K entries in fixture (one 8-K excluded)
        assert all(f.filing_type == "10-K" for f in filings)

    def test_filters_by_filing_type(self):
        apple_data = _load_fixture("submissions_apple.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(apple_data)

        filings = fetch_company_filings(
            APPLE_CIK, ["8-K"], date(2020, 1, 1), client=client
        )

        assert len(filings) == 1
        assert filings[0].filing_type == "8-K"

    def test_filters_by_since_date(self):
        apple_data = _load_fixture("submissions_apple.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(apple_data)

        filings = fetch_company_filings(
            APPLE_CIK, ["10-K"], date(2023, 1, 1), client=client
        )

        # Only 2024 and 2023 10-Ks qualify (2022 and 2021 are excluded)
        assert len(filings) == 2
        assert all(f.filed_date >= date(2023, 1, 1) for f in filings)

    def test_returns_empty_for_no_matches(self):
        apple_data = _load_fixture("submissions_apple.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(apple_data)

        filings = fetch_company_filings(
            APPLE_CIK, ["10-K"], date(2030, 1, 1), client=client
        )

        assert filings == []

    def test_filing_metadata_fields(self):
        apple_data = _load_fixture("submissions_apple.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(apple_data)

        filings = fetch_company_filings(
            APPLE_CIK, ["10-K"], date(2024, 1, 1), client=client
        )

        assert len(filings) == 1
        f = filings[0]
        assert f.cik == APPLE_CIK
        assert f.accession_number == APPLE_ACCESSION
        assert f.filed_date == APPLE_FILED
        assert f.primary_document == APPLE_DOC

    def test_cik_is_zero_padded_in_results(self):
        apple_data = _load_fixture("submissions_apple.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(apple_data)

        filings = fetch_company_filings(
            "320193",  # pass without padding
            ["10-K"],
            date(2020, 1, 1),
            client=client,
        )

        assert all(len(f.cik) == 10 for f in filings)
        assert all(f.cik == APPLE_CIK for f in filings)

    def test_returns_empty_on_http_error(self):
        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = httpx.NetworkError("connection error")

        filings = fetch_company_filings(APPLE_CIK, ["10-K"], date(2020, 1, 1), client=client)

        assert filings == []

    def test_follows_pagination_files(self):
        """Amazon fixture has a 'files' list; old submissions should be fetched."""
        amazon_data = _load_fixture("submissions_amazon.json")
        old_data = _load_fixture("submissions_amazon_old.json")
        client = MagicMock(spec=httpx.Client)
        # First call: submissions, second call: old submissions page
        client.get.side_effect = [
            _make_response(amazon_data),
            _make_response(old_data),
        ]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            filings = fetch_company_filings(
                "0001018724", ["10-K"], date(2010, 1, 1), client=client
            )

        assert client.get.call_count == 2
        # Recent: 3 10-Ks; old page: 2 10-Ks → 5 total
        assert len(filings) == 5
        assert all(f.filing_type == "10-K" for f in filings)

    def test_skips_old_page_on_http_error(self):
        """If the old filings page fails, log a warning and continue."""
        amazon_data = _load_fixture("submissions_amazon.json")

        # Use HTTPStatusError (not NetworkError) so tenacity does NOT retry it
        # — the retry decorator only retries TimeoutException and NetworkError.
        server_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=MagicMock(spec=httpx.Request),
            response=MagicMock(spec=httpx.Response),
        )

        client = MagicMock(spec=httpx.Client)
        # First call: submissions page (success). Second call: old-page (error).
        first_resp = _make_response(amazon_data)
        client.get.side_effect = [first_resp, server_error]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            filings = fetch_company_filings(
                "0001018724", ["10-K"], date(2020, 1, 1), client=client
            )

        # Only recent 10-Ks (since_date 2020 → 3 of them)
        assert len(filings) == 3

    def test_multiple_filing_types(self):
        msft_data = _load_fixture("submissions_microsoft.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(msft_data)

        filings = fetch_company_filings(
            "0000789019", ["10-K", "DEF 14A"], date(2020, 1, 1), client=client
        )

        types = {f.filing_type for f in filings}
        assert "10-K" in types
        assert "DEF 14A" in types
        assert "8-K" not in types


# ===========================================================================
# download_filing
# ===========================================================================


class TestDownloadFiling:
    def _make_s3(self, key_exists: bool = False) -> MagicMock:
        s3 = MagicMock()
        if key_exists:
            body_mock = MagicMock()
            body_mock.read.return_value = b"cached filing text"
            s3.get_object.return_value = {"Body": body_mock}
        else:
            from botocore.exceptions import ClientError

            s3.head_object.side_effect = ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        return s3

    def test_downloads_and_stores_filing(self):
        filing = _apple_filing()
        sample_text = (FIXTURES / "filing_10k_sample.txt").read_text()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(sample_text)
        s3 = self._make_s3(key_exists=False)

        doc = download_filing(filing, client=client, s3_client=s3)

        assert doc.text == sample_text
        assert "edgar/" in doc.object_store_path
        assert _accession_no_dashes(APPLE_ACCESSION) in doc.object_store_path
        s3.put_object.assert_called_once()

    def test_idempotent_skips_download_when_in_object_store(self):
        filing = _apple_filing()
        client = MagicMock(spec=httpx.Client)
        s3 = self._make_s3(key_exists=True)

        doc = download_filing(filing, client=client, s3_client=s3)

        assert doc.text == "cached filing text"
        # HTTP client must NOT be called when we already have the file
        client.get.assert_not_called()
        s3.put_object.assert_not_called()

    def test_correct_object_store_key_format(self):
        filing = _apple_filing()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response("filing text")
        s3 = self._make_s3(key_exists=False)

        doc = download_filing(filing, client=client, s3_client=s3)

        expected_key = f"edgar/{APPLE_CIK}/{_accession_no_dashes(APPLE_ACCESSION)}/full.txt"
        assert doc.object_store_path == expected_key

    def test_raises_on_http_error(self):
        from botocore.exceptions import ClientError

        filing = _apple_filing()
        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = httpx.HTTPStatusError(
            "500",
            request=MagicMock(),
            response=MagicMock(status_code=500),
        )
        s3 = self._make_s3(key_exists=False)

        with pytest.raises(httpx.HTTPStatusError):
            download_filing(filing, client=client, s3_client=s3)

    def test_plain_text_filing_returned_as_text(self):
        """Verify plain-text (non-HTML) filing content is handled correctly."""
        plain_text = "FORM 10-K\n\nRISK FACTORS\n\nThe company faces risks."
        filing = _apple_filing()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(plain_text)
        s3 = self._make_s3(key_exists=False)

        doc = download_filing(filing, client=client, s3_client=s3)

        assert doc.text == plain_text

    def test_html_filing_returned_as_text(self):
        """Verify HTML filing content passes through as raw text."""
        html_text = "<html><body><h1>FORM 10-K</h1><p>Risk factors...</p></body></html>"
        filing = _apple_filing()
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(html_text)
        s3 = self._make_s3(key_exists=False)

        doc = download_filing(filing, client=client, s3_client=s3)

        assert doc.text == html_text


# ===========================================================================
# Rate limiting
# ===========================================================================


class TestRateLimiting:
    def test_sleep_called_between_requests_in_ingest(self, db, sample_entity):
        """ingest_all_10k must call time.sleep between requests."""
        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")
        sample_text = (FIXTURES / "filing_10k_sample.txt").read_text()

        client = MagicMock(spec=httpx.Client)
        # Ticker lookup + submissions + each filing download
        client.get.side_effect = [
            _make_response(tickers_data),  # get_cik_for_ticker
            _make_response(apple_data),    # fetch_company_filings
            _make_response(sample_text),   # download filing 1
            _make_response(sample_text),   # download filing 2
            _make_response(sample_text),   # download filing 3
            _make_response(sample_text),   # download filing 4
        ]

        from botocore.exceptions import ClientError

        s3 = MagicMock()
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=client,
                s3_client=s3,
            )

        # time.sleep must have been called multiple times (one per request)
        assert mock_time.sleep.call_count >= 2

    def test_request_delay_constant_exists_and_is_positive(self):
        import cam.ingestion.edgar as edgar_mod

        assert edgar_mod.REQUEST_DELAY > 0
        assert edgar_mod.REQUEST_DELAY <= 0.15  # must respect 10 req/s


# ===========================================================================
# ingest_all_10k
# ===========================================================================


class TestIngestAll10k:
    def _s3_no_objects(self):
        from botocore.exceptions import ClientError

        s3 = MagicMock()
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        return s3

    def test_ingests_10k_filings_for_entity(self, db, sample_entity):
        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")
        sample_text = "FORM 10-K content"

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _make_response(tickers_data),
            _make_response(apple_data),
            _make_response(sample_text),
            _make_response(sample_text),
            _make_response(sample_text),
            _make_response(sample_text),
        ]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
            )

        assert result.ingested == 4
        assert result.skipped == 0
        assert result.errors == 0
        assert result.total == 4

    def test_skips_already_ingested_filings(self, db, sample_entity):
        """Running ingest twice must not create duplicate Event rows."""
        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")
        sample_text = "FORM 10-K content"

        def _fresh_client():
            client = MagicMock(spec=httpx.Client)
            client.get.side_effect = [
                _make_response(tickers_data),
                _make_response(apple_data),
                _make_response(sample_text),
                _make_response(sample_text),
                _make_response(sample_text),
                _make_response(sample_text),
            ]
            return client

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            # First run
            result1 = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=_fresh_client(),
                s3_client=self._s3_no_objects(),
            )

        # Second run — filings now exist in DB
        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result2 = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=_fresh_client(),
                s3_client=self._s3_no_objects(),
            )

        assert result1.ingested == 4
        assert result2.skipped == 4
        assert result2.ingested == 0

        # DB must have exactly 4 event rows
        events = db.execute(select(Event)).scalars().all()
        assert len(events) == 4

    def test_handles_missing_cik_gracefully(self, db):
        """Entity with unknown ticker is counted as error, not crash."""
        entity = Entity(
            id=uuid.uuid4(),
            canonical_name="Ghost Corp",
            ticker="XXXX",
        )
        db.add(entity)
        db.commit()

        tickers_data = _load_fixture("company_tickers.json")
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(tickers_data)

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[entity.id],
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
            )

        assert result.errors == 1
        assert result.ingested == 0
        assert "XXXX" in result.error_details[0]

    def test_skips_entities_without_ticker(self, db):
        """Entities with no ticker are silently skipped."""
        entity = Entity(
            id=uuid.uuid4(),
            canonical_name="Private Co",
            ticker=None,
        )
        db.add(entity)
        db.commit()

        client = MagicMock(spec=httpx.Client)

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[entity.id],
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
            )

        assert result.total == 0
        assert result.ingested == 0
        assert result.errors == 0
        client.get.assert_not_called()

    def test_entity_id_filter_limits_scope(self, db):
        """entity_ids parameter restricts which entities are processed."""
        apple = Entity(id=uuid.uuid4(), canonical_name="Apple Inc.", ticker="AAPL")
        msft = Entity(id=uuid.uuid4(), canonical_name="Microsoft Corp", ticker="MSFT")
        db.add_all([apple, msft])
        db.commit()

        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")
        sample_text = "10-K content"

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _make_response(tickers_data),
            _make_response(apple_data),
            _make_response(sample_text),
            _make_response(sample_text),
            _make_response(sample_text),
            _make_response(sample_text),
        ]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[apple.id],  # only Apple
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
            )

        # Only Apple processed — MSFT not included
        assert result.ingested == 4

    def test_custom_filing_types_respected(self, db, sample_entity):
        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")
        sample_text = "8-K content"

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _make_response(tickers_data),
            _make_response(apple_data),
            _make_response(sample_text),
        ]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
                filing_types=["8-K"],
            )

        assert result.ingested == 1

    def test_event_row_contains_correct_fields(self, db, sample_entity):
        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")
        sample_text = "10-K text"

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _make_response(tickers_data),
            _make_response(apple_data),
            _make_response(sample_text),
            _make_response(sample_text),
            _make_response(sample_text),
            _make_response(sample_text),
        ]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            ingest_all_10k(
                date(2024, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
            )

        events = db.execute(select(Event).where(Event.source == "sec_edgar")).scalars().all()
        assert len(events) == 1
        ev = events[0]
        assert ev.entity_id == sample_entity.id
        assert ev.source == "sec_edgar"
        assert ev.event_type == "filing"
        assert ev.event_date == date(2024, 2, 2)
        assert ev.penalty_usd is None
        assert "10-K" in ev.description
        assert ev.raw_url is not None
        assert ev.raw_json is not None
        assert ev.raw_json["filing_type"] == "10-K"
        assert ev.raw_json["cik"] == APPLE_CIK

    def test_handles_download_error_gracefully(self, db, sample_entity):
        tickers_data = _load_fixture("company_tickers.json")
        apple_data = _load_fixture("submissions_apple.json")

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [
            _make_response(tickers_data),
            _make_response(apple_data),
            # All filing downloads fail
            httpx.NetworkError("connection refused"),
            httpx.NetworkError("connection refused"),
            httpx.NetworkError("connection refused"),
            httpx.NetworkError("connection refused"),
        ]

        with patch("cam.ingestion.edgar.time") as mock_time:
            mock_time.sleep.return_value = None
            result = ingest_all_10k(
                date(2020, 1, 1),
                entity_ids=[sample_entity.id],
                db=db,
                client=client,
                s3_client=self._s3_no_objects(),
            )

        assert result.errors == 4
        assert result.ingested == 0


# ===========================================================================
# Helper function unit tests
# ===========================================================================


class TestHelpers:
    def test_accession_no_dashes(self):
        assert _accession_no_dashes("0000320193-24-000006") == "000032019324000006"

    def test_filing_url_format(self):
        url = _filing_url("0000320193", "0000320193-24-000006", "aapl-20231230.htm")
        assert url.startswith("https://www.sec.gov/Archives/edgar/data/")
        assert "000032019324000006" in url
        assert "aapl-20231230.htm" in url
        # CIK in URL must NOT have leading zeros
        assert "/320193/" in url

    def test_object_store_key_format(self):
        key = _object_store_key("0000320193", "0000320193-24-000006")
        assert key.startswith("edgar/")
        assert "0000320193" in key
        assert "000032019324000006" in key
        assert key.endswith("/full.txt")

    def test_object_exists_returns_true(self):
        s3 = MagicMock()
        s3.head_object.return_value = {}
        assert _object_exists(s3, "bucket", "key") is True

    def test_object_exists_returns_false_on_404(self):
        from botocore.exceptions import ClientError

        s3 = MagicMock()
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        assert _object_exists(s3, "bucket", "key") is False

    def test_object_exists_reraises_other_errors(self):
        from botocore.exceptions import ClientError

        s3 = MagicMock()
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
        )
        with pytest.raises(ClientError):
            _object_exists(s3, "bucket", "key")

    def test_filing_in_db_returns_false_when_absent(self, db):
        assert _filing_in_db(db, "0000320193-24-000006") is False

    def test_filing_in_db_returns_true_when_present(self, db, sample_entity):
        filing = _apple_filing()
        filing.entity_id = sample_entity.id
        key = _object_store_key(filing.cik, filing.accession_number)
        _upsert_filing_event(db, filing, key, sample_entity.id)

        assert _filing_in_db(db, APPLE_ACCESSION) is True

    def test_upsert_filing_event_creates_event_row(self, db, sample_entity):
        filing = _apple_filing()
        filing.entity_id = sample_entity.id
        key = _object_store_key(filing.cik, filing.accession_number)

        _upsert_filing_event(db, filing, key, sample_entity.id)

        events = db.execute(select(Event)).scalars().all()
        assert len(events) == 1
        ev = events[0]
        assert ev.source == "sec_edgar"
        assert ev.event_type == "filing"
        assert ev.event_date == APPLE_FILED
        assert ev.raw_json["accession_number"] == APPLE_ACCESSION

    def test_upsert_filing_event_is_callable_twice_without_error(self, db, sample_entity):
        """Calling _upsert_filing_event twice does not crash (idempotent)."""
        filing = _apple_filing()
        filing.entity_id = sample_entity.id
        key = _object_store_key(filing.cik, filing.accession_number)

        _upsert_filing_event(db, filing, key, sample_entity.id)
        _upsert_filing_event(db, filing, key, sample_entity.id)

        # Two rows are created (each call is independent; DB-level idempotency
        # is handled by _filing_in_db check in ingest_all_10k)
        events = db.execute(select(Event)).scalars().all()
        assert len(events) == 2


# ===========================================================================
# Performance test
# ===========================================================================


class TestPerformance:
    def test_fetch_company_filings_handles_large_recent_block(self):
        """fetch_company_filings must handle hundreds of entries without issue."""
        n = 500
        data = {
            "filings": {
                "recent": {
                    "form": ["10-K"] * n,
                    "accessionNumber": [f"0000320193-{i:02d}-{i:06d}" for i in range(n)],
                    "filingDate": ["2023-01-01"] * n,
                    "primaryDocument": [f"doc-{i}.htm" for i in range(n)],
                },
                "files": [],
            }
        }
        client = MagicMock(spec=httpx.Client)
        client.get.return_value = _make_response(data)

        import time as _time

        start = _time.monotonic()
        filings = fetch_company_filings(
            APPLE_CIK, ["10-K"], date(2020, 1, 1), client=client
        )
        elapsed = _time.monotonic() - start

        assert len(filings) == n
        assert elapsed < 2.0, f"fetch took {elapsed:.2f}s, should be <2s"
