"""
Shared base types for all CAM ingestion modules.

Import IngestResult from here rather than defining it locally in each module.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field


@dataclass
class IngestResult:
    """Summary of a bulk ingestion run.

    Attributes
    ----------
    total:         Total records considered (after date filtering, before idempotency check).
    ingested:      Records successfully written to the DB.
    skipped:       Records already present (idempotency check passed).
    errors:        Records that failed and were sent to the DLQ.
    error_details: Human-readable descriptions of failures (for logs/CLI output).
    dlq_ids:       UUIDs of ingest_failures rows created during this run.
    run_id:        Unique identifier for this ingestion run (links checkpoints and DLQ rows).
    checkpoint:    Last cursor saved before the run completed (None if no checkpointing used).
    """

    total: int = 0
    ingested: int = 0
    skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    dlq_ids: list[uuid.UUID] = field(default_factory=list)
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    checkpoint: dict | None = None
