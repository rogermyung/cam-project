"""
M15 — Ingestion Checkpointing

Saves progress cursors for long-running ingestion runs so a restart can
resume from the last saved position rather than re-processing everything.

Each ingestion source controls its own cursor format::

    OSHA (CSV):     {"offset": 1500}
    CFPB (pages):   {"page": 7, "total_pages": 42}
    EDGAR (index):  {"quarter": "2025Q3"}
    WARN (states):  {"state": "CA"}

Usage::

    from cam.ingestion.checkpoint import save_checkpoint, load_checkpoint, complete_checkpoint

    run_id = uuid.uuid4()
    cursor = load_checkpoint(db, source="osha", run_id=run_id)  # None → start fresh
    start_offset = cursor.get("offset", 0) if cursor else 0

    for i, record in enumerate(records[start_offset:], start=start_offset):
        process(record)
        if i % CHECKPOINT_EVERY == 0:
            save_checkpoint(db, "osha", run_id, {"offset": i}, ok, err)

    complete_checkpoint(db, "osha", run_id)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from cam.db.models import IngestCheckpoint


def save_checkpoint(
    db: Session,
    source: str,
    run_id: uuid.UUID,
    cursor: dict,
    records_ok: int,
    records_err: int,
) -> None:
    """Upsert a progress checkpoint for the given source + run_id.

    Calls ``db.flush()`` but does NOT commit — the caller owns the transaction.
    """
    now = datetime.now(tz=UTC)

    existing = (
        db.execute(
            select(IngestCheckpoint)
            .where(IngestCheckpoint.source == source)
            .where(IngestCheckpoint.run_id == run_id)
            .limit(1)
        )
        .scalars()
        .first()
    )

    if existing is not None:
        existing.checkpoint = cursor
        existing.records_ok = records_ok
        existing.records_err = records_err
        existing.updated_at = now
    else:
        db.add(
            IngestCheckpoint(
                id=uuid.uuid4(),
                source=source,
                run_id=run_id,
                checkpoint=cursor,
                records_ok=records_ok,
                records_err=records_err,
                started_at=now,
                updated_at=now,
            )
        )
    db.flush()


def load_checkpoint(
    db: Session,
    source: str,
    run_id: uuid.UUID | None = None,
) -> dict | None:
    """Return the cursor dict from the latest incomplete checkpoint for this source.

    If ``run_id`` is provided, look up that specific run.
    If ``run_id`` is None, return the most-recently-updated incomplete run.
    Returns None if no incomplete checkpoint is found (start from the beginning).
    """
    row = _load_checkpoint_row(db, source, run_id)
    return row.checkpoint if row is not None else None


def _load_checkpoint_row(
    db: Session,
    source: str,
    run_id: uuid.UUID | None = None,
) -> IngestCheckpoint | None:
    """Return the full IngestCheckpoint row (or None) for the latest incomplete run."""
    stmt = (
        select(IngestCheckpoint)
        .where(IngestCheckpoint.source == source)
        .where(IngestCheckpoint.completed_at.is_(None))
        .order_by(IngestCheckpoint.updated_at.desc())
        .limit(1)
    )
    if run_id is not None:
        stmt = stmt.where(IngestCheckpoint.run_id == run_id)

    return db.execute(stmt).scalars().first()


def complete_checkpoint(
    db: Session,
    source: str,
    run_id: uuid.UUID,
) -> None:
    """Mark a run's checkpoint as completed.

    Calls ``db.flush()`` but does NOT commit.
    """
    row = (
        db.execute(
            select(IngestCheckpoint)
            .where(IngestCheckpoint.source == source)
            .where(IngestCheckpoint.run_id == run_id)
            .limit(1)
        )
        .scalars()
        .first()
    )

    if row is not None:
        row.completed_at = datetime.now(tz=UTC)
        db.flush()
