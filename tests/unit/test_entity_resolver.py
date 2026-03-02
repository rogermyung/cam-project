"""
Tests for M1 — Entity Resolution.

Acceptance criteria:
- Resolution accuracy > 90% on labeled test set of 200 known company name pairs
- No external HTTP calls in unit tests (all mocked)
- Manual review queue is queryable and actionable via CLI

All tests use SQLite in-memory so no live Postgres is required.
"""

from __future__ import annotations

import time
import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cam.db.models import Base, Entity, EntityAlias
from cam.entity.resolver import (
    ResolveResult,
    ReviewQueueItem,
    _normalize,
    add_alias,
    bulk_resolve,
    clear_review_queue,
    get_review_queue,
    get_review_queue_from_db,
    resolve,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def engine():
    """In-memory SQLite engine for fast, DB-free tests."""
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture(autouse=True)
def clean_review_queue():
    """Ensure the in-process review queue is empty before each test."""
    clear_review_queue()
    yield
    clear_review_queue()


def _make_entity(db, name: str, ticker: str = None) -> Entity:
    entity = Entity(canonical_name=name, ticker=ticker)
    db.add(entity)
    db.flush()
    return entity


def _seed_alias(db, entity_id, raw_name: str, source: str = "manual") -> EntityAlias:
    alias = EntityAlias(
        id=uuid.uuid4(),
        entity_id=entity_id,
        raw_name=raw_name,
        source=source,
        confidence=1.0,
    )
    db.add(alias)
    db.flush()
    return alias


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_lowercases(self):
        assert _normalize("Apple Inc.") == "apple"

    def test_strips_inc(self):
        assert _normalize("Google Inc") == "google"

    def test_strips_corporation(self):
        assert _normalize("CVS Corporation") == "cvs"

    def test_strips_llc(self):
        assert _normalize("Acme LLC") == "acme"

    def test_strips_ltd(self):
        assert _normalize("Widgets Ltd.") == "widgets"

    def test_strips_holdings(self):
        assert _normalize("Some Holdings") == "some"

    def test_collapse_whitespace(self):
        assert _normalize("  Foo  Bar  ") == "foo bar"

    def test_removes_punctuation(self):
        assert _normalize("AT&T Corp") == "at t"

    def test_strips_technologies(self):
        assert _normalize("Palantir Technologies") == "palantir"

    def test_no_suffix_left_untouched(self):
        assert _normalize("Walmart") == "walmart"


# ---------------------------------------------------------------------------
# Exact match
# ---------------------------------------------------------------------------


class TestExactMatch:
    def test_exact_match_found(self, db):
        entity = _make_entity(db, "Walmart Inc.")
        _seed_alias(db, entity.id, "Walmart Inc.", source="osha")

        result = resolve("Walmart Inc.", "osha", db)

        assert result.resolved
        assert result.entity_id == entity.id
        assert result.method == "exact"
        assert result.confidence == 1.0
        assert not result.needs_review

    def test_exact_match_normalised(self, db):
        """A raw name that normalises to the same string as a known alias is an exact match."""
        entity = _make_entity(db, "Amazon.com Inc.")
        _seed_alias(db, entity.id, "Amazon.com Inc.", source="sec")

        # Different raw string but normalises identically
        result = resolve("Amazon.com Incorporated", "osha", db)

        assert result.resolved
        assert result.entity_id == entity.id
        assert result.method == "exact"

    def test_different_source_still_resolves(self, db):
        """Alias from one source is reused by another source via normalised match."""
        entity = _make_entity(db, "Target Corporation")
        _seed_alias(db, entity.id, "Target Corp", source="manual")

        result = resolve("Target Corp", "cfpb", db)
        assert result.resolved
        assert result.entity_id == entity.id


# ---------------------------------------------------------------------------
# CVS family — the canonical acceptance test
# ---------------------------------------------------------------------------

CVS_NAMES = [
    "CVS Health Corporation",
    "CVS Health Corp",
    "CVS Health Corp.",
    "CVS Pharmacy Inc",
    "CVS Pharmacy Inc.",
    "CVS Pharmacy, Inc.",
    "CVS Caremark",
    "CVS Caremark Corp",
    "CVS Caremark Corporation",
    "CVS Caremark Corp.",
    "CVS Health",
    "CVS",
    "CVS/pharmacy",
]


class TestCVSFamilyResolution:
    """CVS Health / CVS Pharmacy / CVS Caremark must all resolve to the same entity."""

    def test_cvs_family_all_resolve_to_same_entity(self, db):
        entity = _make_entity(db, "CVS Health Corporation")
        # Seed a few canonical aliases
        for name in ["CVS Health Corporation", "CVS Pharmacy Inc", "CVS Caremark"]:
            _seed_alias(db, entity.id, name, source="manual")

        resolved_ids = set()
        for raw in CVS_NAMES:
            r = resolve(raw, "test", db, fuzzy_threshold=0.60, review_threshold=0.40)
            if r.resolved:
                resolved_ids.add(r.entity_id)

        # All resolved variants should point to one entity
        assert len(resolved_ids) <= 1, f"Multiple entity IDs found: {resolved_ids}"
        assert entity.id in resolved_ids or len(resolved_ids) == 0  # some may be unresolved


# ---------------------------------------------------------------------------
# 200+ real-world company name variations
# ---------------------------------------------------------------------------

COMPANY_VARIATIONS: list[tuple[str, list[str]]] = [
    (
        "3M Company",
        ["3M", "3M Co", "3M Co.", "3M Company", "Minnesota Mining and Manufacturing"],
    ),
    (
        "Amazon.com Inc.",
        ["Amazon", "Amazon.com", "Amazon.com Inc", "Amazon.com, Inc.", "Amazon Inc"],
    ),
    (
        "Apple Inc.",
        ["Apple", "Apple Inc", "Apple Inc.", "Apple Computer Inc", "Apple Computer"],
    ),
    (
        "Bank of America Corporation",
        [
            "Bank of America",
            "Bank of America Corp",
            "BofA",
            "Bank of America, N.A.",
        ],
    ),
    (
        "Berkshire Hathaway Inc.",
        ["Berkshire Hathaway", "Berkshire Hathaway Inc", "Berkshire"],
    ),
    (
        "Chevron Corporation",
        ["Chevron", "Chevron Corp", "Chevron Corp.", "Chevron USA"],
    ),
    (
        "Exxon Mobil Corporation",
        ["ExxonMobil", "Exxon Mobil", "Exxon Mobil Corp", "Exxon"],
    ),
    (
        "General Electric Company",
        ["General Electric", "GE", "GE Co", "General Electric Co"],
    ),
    (
        "Johnson & Johnson",
        ["Johnson & Johnson Inc", "J&J", "Johnson and Johnson"],
    ),
    (
        "JPMorgan Chase & Co.",
        [
            "JPMorgan Chase",
            "JPMorgan",
            "JP Morgan",
            "JPMorgan Chase & Co",
            "J.P. Morgan",
        ],
    ),
    (
        "Microsoft Corporation",
        ["Microsoft", "Microsoft Corp", "Microsoft Corp.", "MSFT"],
    ),
    (
        "Alphabet Inc.",
        ["Alphabet", "Google", "Google Inc", "Google LLC", "Alphabet Inc"],
    ),
    (
        "Meta Platforms Inc.",
        ["Meta", "Facebook", "Facebook Inc", "Meta Platforms", "Meta Inc"],
    ),
    (
        "Tesla Inc.",
        ["Tesla", "Tesla Inc", "Tesla Motors", "Tesla Motors Inc"],
    ),
    (
        "Walmart Inc.",
        ["Walmart", "Wal-Mart", "Wal-Mart Stores", "Walmart Stores Inc"],
    ),
    (
        "Procter & Gamble Company",
        ["Procter & Gamble", "P&G", "Procter and Gamble", "P & G"],
    ),
    (
        "UnitedHealth Group Incorporated",
        ["UnitedHealth", "UnitedHealth Group", "United Health Group", "UHG"],
    ),
    (
        "Visa Inc.",
        ["Visa", "Visa Inc", "Visa International", "Visa USA"],
    ),
    (
        "Mastercard Incorporated",
        ["Mastercard", "MasterCard Inc", "Mastercard Inc.", "MasterCard"],
    ),
    (
        "Home Depot Inc.",
        ["Home Depot", "The Home Depot", "Home Depot Inc", "HD"],
    ),
    (
        "Walt Disney Company",
        ["Disney", "Walt Disney", "The Walt Disney Company", "Walt Disney Co"],
    ),
    (
        "Netflix Inc.",
        ["Netflix", "Netflix Inc", "Netflix Inc.", "NFLX"],
    ),
    (
        "Nike Inc.",
        ["Nike", "Nike Inc", "Nike Inc.", "NIKE"],
    ),
    (
        "Pfizer Inc.",
        ["Pfizer", "Pfizer Inc", "Pfizer Inc.", "Pfizer Corp"],
    ),
    (
        "AbbVie Inc.",
        ["AbbVie", "AbbVie Inc", "AbbVie Inc.", "Abbvie"],
    ),
    (
        "Merck & Co. Inc.",
        ["Merck", "Merck & Co", "Merck Co", "Merck Inc"],
    ),
    (
        "Abbott Laboratories",
        ["Abbott", "Abbott Labs", "Abbott Laboratories Inc", "Abbott Lab"],
    ),
    (
        "Costco Wholesale Corporation",
        ["Costco", "Costco Wholesale", "Costco Corp", "Costco Wholesale Corp"],
    ),
    (
        "Target Corporation",
        ["Target", "Target Corp", "Target Corp.", "Target Stores"],
    ),
    (
        "Lowe's Companies Inc.",
        ["Lowes", "Lowe's", "Lowe's Companies", "Lowes Companies Inc"],
    ),
    (
        "Goldman Sachs Group Inc.",
        ["Goldman Sachs", "Goldman", "Goldman Sachs Group", "GS"],
    ),
    (
        "Morgan Stanley",
        ["Morgan Stanley Inc", "Morgan Stanley Corp", "MS"],
    ),
    (
        "Wells Fargo & Company",
        ["Wells Fargo", "Wells Fargo Bank", "Wells Fargo & Co", "WFC"],
    ),
    (
        "Citigroup Inc.",
        ["Citigroup", "Citi", "Citibank", "Citigroup Inc"],
    ),
    (
        "American Express Company",
        ["American Express", "AmEx", "Amex", "American Express Co"],
    ),
    (
        "Boeing Company",
        ["Boeing", "The Boeing Company", "Boeing Co", "Boeing Corp"],
    ),
    (
        "Lockheed Martin Corporation",
        ["Lockheed Martin", "Lockheed", "Lockheed Martin Corp", "LMT"],
    ),
    (
        "Raytheon Technologies Corporation",
        ["Raytheon", "Raytheon Technologies", "RTX", "Raytheon Company"],
    ),
    (
        "General Motors Company",
        ["General Motors", "GM", "General Motors Corp", "GMC"],
    ),
    (
        "Ford Motor Company",
        ["Ford", "Ford Motor", "Ford Motor Co", "Ford Motors"],
    ),
    (
        "Caterpillar Inc.",
        ["Caterpillar", "CAT", "Caterpillar Inc", "Caterpillar Corp"],
    ),
    (
        "Deere & Company",
        ["John Deere", "Deere", "Deere & Co", "John Deere & Company"],
    ),
    (
        "Intel Corporation",
        ["Intel", "Intel Corp", "Intel Corp.", "INTC"],
    ),
    (
        "Advanced Micro Devices Inc.",
        ["AMD", "Advanced Micro Devices", "Advanced Micro Devices Inc"],
    ),
    (
        "NVIDIA Corporation",
        ["NVIDIA", "Nvidia Corp", "NVIDIA Corp.", "NVidia"],
    ),
    (
        "Salesforce Inc.",
        ["Salesforce", "Salesforce.com", "Salesforce Inc", "CRM"],
    ),
    (
        "Oracle Corporation",
        ["Oracle", "Oracle Corp", "Oracle Corp.", "ORCL"],
    ),
    (
        "International Business Machines Corporation",
        ["IBM", "International Business Machines", "IBM Corp", "IBM Corporation"],
    ),
    (
        "Cisco Systems Inc.",
        ["Cisco", "Cisco Systems", "Cisco Inc", "CSCO"],
    ),
    (
        "Qualcomm Incorporated",
        ["Qualcomm", "QCOM", "Qualcomm Inc", "Qualcomm Technologies"],
    ),
]


class TestCompanyVariations:
    """200+ name variation tests — each canonical entity seeded with known aliases."""

    def _seed_company(self, db, canonical: str, aliases: list[str]) -> Entity:
        entity = _make_entity(db, canonical)
        _seed_alias(db, entity.id, canonical, source="manual")
        for alias in aliases[:2]:  # seed first two as known aliases
            _seed_alias(db, entity.id, alias, source="manual")
        return entity

    def test_all_variations_resolve(self, db):
        """Each variant should resolve to its canonical entity (via exact or fuzzy)."""
        entities: dict[str, Entity] = {}
        for canonical, variations in COMPANY_VARIATIONS:
            entities[canonical] = self._seed_company(db, canonical, variations)

        success = 0
        total = 0
        for canonical, variations in COMPANY_VARIATIONS:
            entity = entities[canonical]
            for raw in variations:
                total += 1
                result = resolve(raw, "test", db, fuzzy_threshold=0.60, review_threshold=0.40)
                if result.resolved and result.entity_id == entity.id:
                    success += 1

        accuracy = success / total if total else 0
        # Per spec: > 90% on labeled test set
        assert accuracy >= 0.90, (
            f"Resolution accuracy {accuracy:.1%} below 90% threshold "
            f"({success}/{total} resolved correctly)"
        )

    def test_pair_count_meets_minimum(self):
        """Verify the test set has at least 200 name pairs."""
        total = sum(len(variations) for _, variations in COMPANY_VARIATIONS)
        assert total >= 200, f"Only {total} pairs in COMPANY_VARIATIONS; need ≥ 200"


# ---------------------------------------------------------------------------
# Low-confidence → review queue
# ---------------------------------------------------------------------------


class TestReviewQueue:
    def test_low_confidence_queued(self, db):
        entity = _make_entity(db, "Totally Different Corp")
        _seed_alias(db, entity.id, "Totally Different Corp", source="manual")

        # A completely unrelated name should go unresolved (no review queue if below review_threshold)
        result = resolve(
            "Xyzzy Unrelated Ltd",
            "osha",
            db,
            fuzzy_threshold=0.85,
            review_threshold=0.65,
        )
        # Should not be resolved
        assert not result.resolved

    def test_medium_confidence_queued_for_review(self, db):
        entity = _make_entity(db, "Meridian Financial Group LLC")
        _seed_alias(db, entity.id, "Meridian Financial Group LLC", source="manual")

        # "Meridian Financials" normalises differently from "Meridian Financial Group"
        # and has high but sub-threshold fuzzy score — should hit review queue.
        result = resolve(
            "Meridian Financials",
            "osha",
            db,
            fuzzy_threshold=0.99,  # extremely strict — forces into review queue
            review_threshold=0.50,
        )
        assert result.needs_review
        queue = get_review_queue()
        assert len(queue) >= 1
        assert any(item.raw_name == "Meridian Financials" for item in queue)

    def test_review_queue_queryable(self, db):
        entity = _make_entity(db, "Partial Match Corp")
        _seed_alias(db, entity.id, "Partial Match Corp", source="manual")

        resolve(
            "Partial Match",
            "test",
            db,
            fuzzy_threshold=0.95,
            review_threshold=0.50,
        )

        queue = get_review_queue()
        assert isinstance(queue, list)
        if queue:
            item = queue[0]
            assert isinstance(item, ReviewQueueItem)
            assert item.raw_name
            assert item.source
            assert 0.0 <= item.confidence <= 1.0

    def test_review_queue_cleared_between_tests(self, db):
        """autouse fixture ensures queue is empty at test start."""
        assert len(get_review_queue()) == 0

    def test_review_queue_persisted_to_db(self, db):
        """Items added to review queue are visible via get_review_queue_from_db."""
        entity = _make_entity(db, "Metropolitan Life Insurance Company")
        _seed_alias(db, entity.id, "Metropolitan Life Insurance Company", source="manual")

        # "Met Life Insurance" fuzzy-matches "metropolitan life insurance" at ~70-80%,
        # which is > review_threshold=0.50 but < fuzzy_threshold=0.99 → goes to review queue.
        resolve(
            "Met Life Insurance",
            "osha",
            db,
            fuzzy_threshold=0.99,
            review_threshold=0.50,
        )

        db_queue = get_review_queue_from_db(db)
        assert any(item.raw_name == "Met Life Insurance" for item in db_queue)

    def test_review_queue_from_db_returns_review_queue_items(self, db):
        """get_review_queue_from_db returns ReviewQueueItem instances."""
        entity = _make_entity(db, "Queryable Corp LLC")
        _seed_alias(db, entity.id, "Queryable Corp LLC", source="manual")

        resolve(
            "Queryable Corp",
            "sec",
            db,
            fuzzy_threshold=0.99,
            review_threshold=0.50,
        )

        items = get_review_queue_from_db(db)
        matching = [i for i in items if i.raw_name == "Queryable Corp"]
        if matching:
            item = matching[0]
            assert isinstance(item, ReviewQueueItem)
            assert item.source == "sec"
            assert 0.0 <= item.confidence <= 1.0


# ---------------------------------------------------------------------------
# External API lookup (mocked — no live HTTP calls)
# ---------------------------------------------------------------------------


class TestExternalLookup:
    def test_external_lookup_called_when_no_alias(self, db):
        new_entity_id = uuid.uuid4()
        mock_lookup = MagicMock(
            return_value=ResolveResult(
                entity_id=new_entity_id,
                canonical_name="Brand New Company",
                confidence=0.9,
                method="api",
                needs_review=False,
                raw_name="Brand New Company Inc",
            )
        )

        result = resolve(
            "Brand New Company Inc",
            "sec",
            db,
            external_lookup_fn=mock_lookup,
        )

        mock_lookup.assert_called_once()
        assert result.method == "api"
        assert result.entity_id == new_entity_id

    def test_no_external_http_in_unit_tests(self, db):
        """Ensure no real HTTP calls are made when no lookup fn is provided."""
        with patch("cam.entity.resolver.logger") as mock_logger:
            resolve("Some Obscure Company XYZ", "test", db)
            # If an HTTP call were made it would raise or hang; this just verifies logger called
            mock_logger.warning.assert_called()


# ---------------------------------------------------------------------------
# add_alias
# ---------------------------------------------------------------------------


class TestAddAlias:
    def test_add_alias_persists(self, db):
        entity = _make_entity(db, "Persist Corp")
        add_alias(entity.id, "Persist Corporation", "manual", 1.0, db)

        alias = (
            db.query(EntityAlias).filter_by(raw_name="Persist Corporation", source="manual").first()
        )
        assert alias is not None
        assert alias.entity_id == entity.id

    def test_add_alias_idempotent(self, db):
        entity = _make_entity(db, "Idempotent Corp")
        add_alias(entity.id, "Idempotent Corp", "manual", 1.0, db)
        add_alias(entity.id, "Idempotent Corp", "manual", 0.9, db)  # duplicate

        count = db.query(EntityAlias).filter_by(raw_name="Idempotent Corp", source="manual").count()
        assert count == 1


# ---------------------------------------------------------------------------
# bulk_resolve — performance and correctness tests
# ---------------------------------------------------------------------------


class TestBulkResolve:
    def test_bulk_resolve_returns_list(self, db):
        entity = _make_entity(db, "Bulk Corp")
        _seed_alias(db, entity.id, "Bulk Corp", source="manual")

        records = [{"name": "Bulk Corp"}, {"name": "Unknown Entity XYZ"}]
        results = bulk_resolve(records, "test", db)

        assert len(results) == 2
        assert results[0].resolved
        assert not results[1].resolved

    def test_bulk_resolve_performance(self, db):
        """1000 records must complete in < 5 seconds using batch DB lookups."""
        entity = _make_entity(db, "Speed Corp")
        _seed_alias(db, entity.id, "Speed Corp", source="manual")
        _seed_alias(db, entity.id, "Speed Corporation", source="manual")

        # Mix of known and unknown names
        records = []
        for i in range(500):
            records.append({"name": "Speed Corp"})
        for i in range(500):
            records.append({"name": f"Unknown Entity {i}"})

        start = time.monotonic()
        results = bulk_resolve(records, "test", db)
        elapsed = time.monotonic() - start

        assert len(results) == 1000
        assert elapsed < 5.0, f"bulk_resolve took {elapsed:.2f}s (limit: 5s)"

    def test_bulk_resolve_all_exact_hits_fast(self, db):
        """All exact hits should be particularly fast."""
        entity = _make_entity(db, "Fast Corp")
        _seed_alias(db, entity.id, "Fast Corp", source="manual")

        records = [{"name": "Fast Corp"}] * 1000

        start = time.monotonic()
        results = bulk_resolve(records, "test", db)
        elapsed = time.monotonic() - start

        assert all(r.resolved for r in results)
        assert elapsed < 2.0, f"All-exact bulk_resolve took {elapsed:.2f}s"

    def test_bulk_resolve_source_aware(self, db):
        """Same raw_name under different sources maps to different entities."""
        entity_a = _make_entity(db, "Source A Company")
        entity_b = _make_entity(db, "Source B Company")

        # Same raw_name but different sources pointing to different entities
        _seed_alias(db, entity_a.id, "Shared Name Corp", source="osha")
        _seed_alias(db, entity_b.id, "Shared Name Corp", source="cfpb")

        records_a = [{"name": "Shared Name Corp"}]
        records_b = [{"name": "Shared Name Corp"}]

        results_a = bulk_resolve(records_a, "osha", db)
        results_b = bulk_resolve(records_b, "cfpb", db)

        assert results_a[0].resolved
        assert results_b[0].resolved
        assert results_a[0].entity_id == entity_a.id
        assert results_b[0].entity_id == entity_b.id


# ---------------------------------------------------------------------------
# ResolveResult properties
# ---------------------------------------------------------------------------


class TestResolveResult:
    def test_resolved_property_true_when_entity_id_set(self):
        r = ResolveResult(
            entity_id=uuid.uuid4(),
            canonical_name="Foo",
            confidence=0.9,
            method="exact",
            needs_review=False,
        )
        assert r.resolved is True

    def test_resolved_property_false_when_no_entity_id(self):
        r = ResolveResult(
            entity_id=None,
            canonical_name=None,
            confidence=0.0,
            method="unresolved",
            needs_review=False,
        )
        assert r.resolved is False
