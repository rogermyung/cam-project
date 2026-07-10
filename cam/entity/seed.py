"""
Seed the entities table from SEC EDGAR's company_tickers.json endpoint.

This is a one-time (idempotent) bootstrap step required before running the
ingestion pipeline.  The pipeline sources (EPA, CFPB, OSHA, WARN) resolve
raw company names against existing Entity rows via bulk_resolve; EDGAR
requires Entity rows with tickers.  Without this seed, all ingestion produces
orphaned Events with entity_id=NULL.

An EntityAlias row (source="sec_seed", confidence=1.0) is created alongside
each Entity so that bulk_resolve can fuzzy-match raw company names from
regulatory filings against the canonical SEC name.

Usage::

    python -m cam.entrypoint seed
    python -m cam.entrypoint seed --batch-size 200
    python -m cam.entrypoint seed --dry-run
"""

from __future__ import annotations

import logging
import uuid

import httpx
from sqlalchemy.orm import Session

from cam.db.models import Entity, EntityAlias

logger = logging.getLogger(__name__)

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
DEFAULT_BATCH_SIZE = 500


def fetch_tickers(user_agent: str) -> dict:
    """Fetch company_tickers.json from SEC EDGAR (one HTTP call, ~10 000 companies)."""
    logger.info("Fetching %s", SEC_TICKERS_URL)
    resp = httpx.get(
        SEC_TICKERS_URL,
        headers={"User-Agent": user_agent},
        timeout=30,
        follow_redirects=True,
    )
    resp.raise_for_status()
    data = resp.json()
    logger.info("Fetched %d companies from SEC", len(data))
    return data


def _upsert_batch(db: Session, entities: list[Entity], aliases: list[EntityAlias]) -> None:
    """Insert entities and aliases, skipping any that already exist.

    Uses a dialect-agnostic INSERT OR IGNORE / INSERT … ON CONFLICT DO NOTHING
    approach via SQLAlchemy Core so that the function works with both
    PostgreSQL (production) and SQLite (unit tests).
    """
    from sqlalchemy import text as sa_text

    entity_rows = [
        {"id": str(e.id), "canonical_name": e.canonical_name, "ticker": e.ticker} for e in entities
    ]
    alias_rows = [
        {
            "id": str(a.id),
            "entity_id": str(a.entity_id),
            "raw_name": a.raw_name,
            "source": a.source,
            "confidence": a.confidence,
        }
        for a in aliases
    ]

    # Detect database dialect without relying on the deprecated Session.bind.
    dialect_name = db.connection().engine.dialect.name

    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        if entity_rows:
            db.execute(pg_insert(Entity.__table__).on_conflict_do_nothing(), entity_rows)
        if alias_rows:
            db.execute(pg_insert(EntityAlias.__table__).on_conflict_do_nothing(), alias_rows)
    else:
        # SQLite (unit tests) — INSERT OR IGNORE honours all UNIQUE constraints
        if entity_rows:
            db.execute(
                sa_text(
                    "INSERT OR IGNORE INTO entities (id, canonical_name, ticker)"
                    " VALUES (:id, :canonical_name, :ticker)"
                ),
                entity_rows,
            )
        if alias_rows:
            db.execute(
                sa_text(
                    "INSERT OR IGNORE INTO entity_aliases"
                    " (id, entity_id, raw_name, source, confidence)"
                    " VALUES (:id, :entity_id, :raw_name, :source, :confidence)"
                ),
                alias_rows,
            )

    db.commit()


def seed(
    db: Session,
    tickers: dict,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Seed entities and aliases into *db* from a pre-fetched *tickers* dict.

    Parameters
    ----------
    db:
        An active SQLAlchemy session.  The caller is responsible for opening
        and closing the session (``get_session()`` context manager recommended).
    tickers:
        The parsed JSON response from SEC EDGAR's ``company_tickers.json``
        endpoint (keys are stringified indices, values have ``ticker`` and
        ``title`` fields).
    batch_size:
        Number of rows to accumulate before each DB commit.
    dry_run:
        If True, count entities that *would* be inserted but write nothing.

    Returns
    -------
    (inserted, skipped):
        Count of new rows inserted and rows skipped (already existed or blank).
    """
    from sqlalchemy import text

    # Load all existing tickers/aliases in one query to avoid per-row lookups
    existing_tickers: set[str] = {
        row[0]
        for row in db.execute(
            text("SELECT ticker FROM entities WHERE ticker IS NOT NULL")
        ).fetchall()
    }
    existing_aliases: set[str] = {
        row[0]
        for row in db.execute(
            text("SELECT raw_name FROM entity_aliases WHERE source = 'sec_seed'")
        ).fetchall()
    }
    logger.info(
        "%d entities, %d aliases already in DB — will skip duplicates",
        len(existing_tickers),
        len(existing_aliases),
    )

    inserted = 0
    skipped = 0
    batch_entities: list[Entity] = []
    batch_aliases: list[EntityAlias] = []

    for item in tickers.values():
        name: str = item.get("title", "").strip()
        ticker: str = item.get("ticker", "").strip().upper()

        if not name or not ticker:
            skipped += 1
            continue

        if ticker in existing_tickers or name in existing_aliases:
            skipped += 1
            continue

        entity_id = uuid.uuid4()
        batch_entities.append(
            Entity(
                id=entity_id,
                canonical_name=name,
                ticker=ticker,
            )
        )
        # Seed a canonical alias so bulk_resolve can match raw regulatory
        # company names (e.g. "Apple Inc." in an OSHA inspection) back to
        # this entity via fuzzy matching.
        batch_aliases.append(
            EntityAlias(
                id=uuid.uuid4(),
                entity_id=entity_id,
                raw_name=name,
                source="sec_seed",
                confidence=1.0,
            )
        )
        inserted += 1

        if len(batch_entities) >= batch_size:
            if not dry_run:
                _upsert_batch(db, batch_entities, batch_aliases)
            logger.info(
                "  committed batch — %d inserted so far, %d skipped",
                inserted,
                skipped,
            )
            batch_entities = []
            batch_aliases = []

    # Flush the final partial batch
    if batch_entities and not dry_run:
        _upsert_batch(db, batch_entities, batch_aliases)

    prefix = "[DRY RUN] " if dry_run else ""
    logger.info(
        "%sDone — %d entities inserted, %d skipped (already existed)",
        prefix,
        inserted,
        skipped,
    )
    return inserted, skipped


def run(batch_size: int = DEFAULT_BATCH_SIZE, dry_run: bool = False) -> tuple[int, int]:
    """Fetch tickers from SEC EDGAR and seed the database.

    This is the top-level entry point called by the CLI subcommand.  It reads
    ``EDGAR_USER_AGENT`` and ``DATABASE_URL`` from the application settings.

    Returns
    -------
    (inserted, skipped)
    """
    from cam.config import get_settings
    from cam.db.session import get_session

    cfg = get_settings()
    tickers = fetch_tickers(cfg.edgar_user_agent)

    with get_session() as db:
        return seed(db, tickers, batch_size=batch_size, dry_run=dry_run)
