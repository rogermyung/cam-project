"""
M15 — Dead Letter Queue (DLQ)

Records failed ingestion records so operators can examine and replay them later.
All public functions are best-effort: they never raise, so a DLQ write failure
cannot mask or replace the original ingestion error.

CLI usage::

    python -m cam.ingestion.dlq list [--source osha] [--error-type entity_resolution]
    python -m cam.ingestion.dlq export --output failures.csv
    python -m cam.ingestion.dlq replay --ids <uuid> [<uuid> ...]
    python -m cam.ingestion.dlq replay --source osha --error-type entity_resolution
    python -m cam.ingestion.dlq dismiss --ids <uuid> [<uuid> ...] [--note "reason"]
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import traceback as tb
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from cam.db.models import IngestFailure

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public error types
# ---------------------------------------------------------------------------

ERROR_ENTITY_RESOLUTION = "entity_resolution"
ERROR_VALIDATION = "validation"
ERROR_DB_WRITE = "db_write"
ERROR_API_ERROR = "api_error"

VALID_ERROR_TYPES = {ERROR_ENTITY_RESOLUTION, ERROR_VALIDATION, ERROR_DB_WRITE, ERROR_API_ERROR}


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


@dataclass
class ReplayResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    failure_details: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core DLQ functions
# ---------------------------------------------------------------------------


def record_failure(
    db: Session,
    source: str,
    run_id: uuid.UUID,
    raw_record: dict[str, Any],
    error_type: str,
    exc: Exception,
    raw_key: str | None = None,
) -> IngestFailure | None:
    """Write a failed record to the DLQ.

    Never raises — if the DLQ write itself fails the error is logged to stderr
    and None is returned, so the caller can keep processing other records.

    Parameters
    ----------
    db:         Active SQLAlchemy session.
    source:     Ingestion source name ('osha', 'edgar', 'epa_tri', etc.).
    run_id:     UUID of the current ingestion run.
    raw_record: Full original record as received from the source.
    error_type: One of ERROR_ENTITY_RESOLUTION, ERROR_VALIDATION,
                ERROR_DB_WRITE, or ERROR_API_ERROR.
    exc:        The exception that caused the failure.
    raw_key:    Optional idempotency key from the source (e.g. activity_nr).
    """
    if error_type not in VALID_ERROR_TYPES:
        logger.warning("record_failure: unknown error_type %r — using 'validation'", error_type)
        error_type = ERROR_VALIDATION

    # Emit structured log line regardless of DB outcome so log aggregators can
    # build dashboards without querying the DB.
    logger.error(
        "ingest_failure",
        extra={
            "source": source,
            "run_id": str(run_id),
            "error_type": error_type,
            "raw_key": raw_key,
            "entity_name": raw_record.get("company_name")
            or raw_record.get("estab_name")
            or raw_record.get("name"),
            "error": str(exc),
        },
    )

    try:
        failure = IngestFailure(
            id=uuid.uuid4(),
            source=source,
            run_id=run_id,
            raw_key=raw_key,
            raw_json=raw_record,
            error_type=error_type,
            error_msg=str(exc),
            traceback=tb.format_exc(),
        )
        # Use a SAVEPOINT so that a failed DLQ write (e.g. table missing, DB
        # hiccup) only rolls back the SAVEPOINT — not the outer transaction.
        # Without this, a failed db.flush() leaves the session in
        # PendingRollbackError and breaks all subsequent DB access in the run.
        with db.begin_nested():
            db.add(failure)
        return failure
    except Exception as dlq_exc:  # noqa: BLE001
        # Last resort: print to stderr so the error is not silently lost
        print(
            f"[DLQ] Failed to write DLQ entry for source={source} key={raw_key}: {dlq_exc}",
            file=sys.stderr,
        )
        return None


def open_failures(
    db: Session,
    source: str | None = None,
    error_type: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[IngestFailure]:
    """Return unresolved DLQ entries, newest first.

    Parameters
    ----------
    source:     Filter by ingestion source (optional).
    error_type: Filter by error classification (optional).
    limit:      Maximum rows to return.
    offset:     Skip this many rows (for pagination).
    """
    stmt = (
        select(IngestFailure)
        .where(IngestFailure.resolved_at.is_(None))
        .order_by(IngestFailure.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    if source is not None:
        stmt = stmt.where(IngestFailure.source == source)
    if error_type is not None:
        stmt = stmt.where(IngestFailure.error_type == error_type)
    return list(db.execute(stmt).scalars().all())


def mark_resolved(
    db: Session,
    failure_ids: list[uuid.UUID],
    note: str = "",
) -> int:
    """Mark DLQ entries as resolved.

    Sets ``resolved_at`` to now.  The ``note`` is appended to ``error_msg``
    so there is an audit trail without a separate column.

    Returns the number of rows updated.
    """
    if not failure_ids:
        return 0
    now = datetime.now(tz=UTC)
    stmt = (
        update(IngestFailure)
        .where(IngestFailure.id.in_(failure_ids))
        .where(IngestFailure.resolved_at.is_(None))
        .values(
            resolved_at=now,
            error_msg=IngestFailure.error_msg + (f" [resolved: {note}]" if note else ""),
        )
    )
    result = db.execute(stmt)
    db.flush()
    return result.rowcount


def replay_failures(
    db: Session,
    failure_ids: list[uuid.UUID],
    ingest_fn: Callable[[dict[str, Any], Session], bool],
) -> ReplayResult:
    """Attempt to re-process DLQ entries through the original ingestion function.

    ``ingest_fn(raw_record, db)`` must return True on success or raise on failure.
    retry_count and last_retry are updated regardless of outcome.
    resolved_at is set only on success.

    The caller is responsible for committing the session after this returns.
    """
    result = ReplayResult()
    now = datetime.now(tz=UTC)

    failures = (
        db.execute(select(IngestFailure).where(IngestFailure.id.in_(failure_ids))).scalars().all()
    )

    for failure in failures:
        result.attempted += 1
        failure.retry_count += 1
        failure.last_retry = now

        try:
            with db.begin_nested():  # savepoint — roll back only this attempt on failure
                success = ingest_fn(failure.raw_json, db)
                if success:
                    failure.resolved_at = now
                    result.succeeded += 1
                else:
                    result.failed += 1
                    result.failure_details.append(f"{failure.id}: ingest_fn returned False")
        except Exception as exc:  # noqa: BLE001
            result.failed += 1
            result.failure_details.append(f"{failure.id}: {exc}")

    db.flush()
    return result


# ---------------------------------------------------------------------------
# Export helper
# ---------------------------------------------------------------------------


def export_to_csv(
    db: Session,
    path: Path,
    source: str | None = None,
    error_type: str | None = None,
) -> int:
    """Export open DLQ failures to a CSV file.  Returns row count."""
    failures = open_failures(db, source=source, error_type=error_type, limit=100_000)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "id",
                "source",
                "run_id",
                "raw_key",
                "error_type",
                "error_msg",
                "retry_count",
                "created_at",
            ],
        )
        writer.writeheader()
        for f in failures:
            writer.writerow(
                {
                    "id": str(f.id),
                    "source": f.source,
                    "run_id": str(f.run_id),
                    "raw_key": f.raw_key or "",
                    "error_type": f.error_type,
                    "error_msg": f.error_msg,
                    "retry_count": f.retry_count,
                    "created_at": str(f.created_at),
                }
            )
    return len(failures)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _get_db() -> Session:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from cam.config import get_settings

    engine = create_engine(get_settings().database_url)
    return sessionmaker(bind=engine)()


def _cmd_list(args: argparse.Namespace) -> None:
    db = _get_db()
    failures = open_failures(db, source=args.source or None, error_type=args.error_type or None)
    if not failures:
        print("No open DLQ failures.")
        return
    print(f"{'ID':<38}  {'SOURCE':<12}  {'ERROR_TYPE':<20}  {'KEY':<30}  CREATED")
    print("-" * 120)
    for f in failures:
        print(
            f"{str(f.id):<38}  {f.source:<12}  {f.error_type:<20}  "
            f"{(f.raw_key or ''):<30}  {f.created_at}"
        )


def _cmd_export(args: argparse.Namespace) -> None:
    db = _get_db()
    path = Path(args.output)
    count = export_to_csv(db, path, source=args.source or None, error_type=args.error_type or None)
    print(f"Exported {count} rows to {path}")


def _cmd_dismiss(args: argparse.Namespace) -> None:
    db = _get_db()
    ids = [uuid.UUID(i) for i in args.ids]
    count = mark_resolved(db, ids, note=args.note or "")
    db.commit()
    print(f"Dismissed {count} DLQ entries.")


def _cmd_replay(args: argparse.Namespace) -> None:
    print("Replay requires a source-specific ingest_fn. Use the Python API directly.")
    sys.exit(1)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m cam.ingestion.dlq",
        description="CAM ingestion dead letter queue management",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # list
    p_list = sub.add_parser("list", help="Show open DLQ failures")
    p_list.add_argument("--source", help="Filter by source (e.g. osha)")
    p_list.add_argument("--error-type", help="Filter by error type")

    # export
    p_export = sub.add_parser("export", help="Export open failures to CSV")
    p_export.add_argument("--output", required=True, help="Output CSV path")
    p_export.add_argument("--source", help="Filter by source")
    p_export.add_argument("--error-type", help="Filter by error type")

    # dismiss
    p_dismiss = sub.add_parser("dismiss", help="Mark DLQ entries as resolved")
    p_dismiss.add_argument("--ids", nargs="+", required=True, help="UUIDs to dismiss")
    p_dismiss.add_argument("--note", help="Reason for dismissal")

    # replay (stub)
    p_replay = sub.add_parser("replay", help="Replay DLQ entries (Python API only)")
    p_replay.add_argument("--ids", nargs="+", help="UUIDs to replay")

    return parser


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()
    dispatch = {
        "list": _cmd_list,
        "export": _cmd_export,
        "dismiss": _cmd_dismiss,
        "replay": _cmd_replay,
    }
    dispatch[args.command](args)
