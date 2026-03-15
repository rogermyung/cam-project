"""
M14 — Output Layer: Static site export and weekly digest.

``export_static_site`` reads the ``alert_scores``, ``entities``, and
``signals`` tables and writes a self-contained directory of JSON files plus
minimal HTML dashboard pages.  The output can be served from any static host
(S3, GitHub Pages, Netlify) or a local ``python -m http.server`` — no live
database is required at serve time.

``export_digest`` produces a plaintext weekly email body summarising new
critical/elevated alerts and top sectors by average composite score.  The
caller is responsible for SMTP delivery.

Directory layout written by export_static_site::

    {output_dir}/
    ├── meta.json          # exported_at, entity_count, alert_count, version
    ├── alerts.json        # all alerts sorted: critical → elevated → watch, date desc
    ├── entities.json      # all entity summaries with current scores
    ├── entities/
    │   └── {id}.json      # per-entity: score history, component breakdown, evidence
    ├── index.html         # alert feed dashboard
    ├── entity.html        # entity detail page  (uses ?id= URL param)
    └── industries.html    # entities grouped by 2-digit NAICS

All files are written atomically (temp file → rename) so a partial export is
never visible to readers.  Re-running the export is idempotent.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from cam.db.models import AlertScore, Entity, Signal
from cam.output._templates import HTML_PAGES

logger = logging.getLogger(__name__)

_VERSION = "1"

# Alert-level ordering for sort: lower = higher priority
_LEVEL_ORDER: dict[str | None, int] = {
    "critical": 0,
    "elevated": 1,
    "watch": 2,
    None: 99,
}

# Maximum score history rows per entity included in the per-entity JSON file
_HISTORY_LIMIT = 90

# Maximum top-evidence items per entity included in the per-entity JSON file
_EVIDENCE_LIMIT = 5


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _write_atomic(path: Path, data: Any) -> None:
    """Write *data* as pretty-printed JSON to *path* atomically.

    Uses a sibling ``.tmp`` file then renames to the target so readers never
    see a partial file.  The parent directory is created if absent.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, default=str, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _fetch_current_scores(db: Session) -> list[AlertScore]:
    """Return the most recent AlertScore for each entity.

    Uses a GROUP BY subquery so only one row per entity is returned even
    when multiple scoring runs have been performed.
    """
    subq = (
        select(
            AlertScore.entity_id,
            func.max(AlertScore.score_date).label("max_date"),
        )
        .group_by(AlertScore.entity_id)
        .subquery()
    )
    stmt = select(AlertScore).join(
        subq,
        (AlertScore.entity_id == subq.c.entity_id) & (AlertScore.score_date == subq.c.max_date),
    )
    return list(db.scalars(stmt).all())


def _fetch_score_history(db: Session) -> dict[Any, list[dict]]:
    """Return score history for all entities, keyed by entity_id.

    At most ``_HISTORY_LIMIT`` rows per entity are kept (most recent first).
    """
    stmt = select(AlertScore).order_by(
        AlertScore.entity_id,
        AlertScore.score_date.desc(),
    )
    history: dict[Any, list[dict]] = defaultdict(list)
    for row in db.scalars(stmt).all():
        bucket = history[row.entity_id]
        if len(bucket) < _HISTORY_LIMIT:
            bucket.append(
                {
                    "score_date": str(row.score_date),
                    "composite_score": row.composite_score,
                    "alert_level": row.alert_level,
                }
            )
    return dict(history)


def _fetch_top_evidence(db: Session) -> dict[Any, list[dict]]:
    """Return the top evidence signals for each entity, keyed by entity_id.

    Signals are ordered by score descending so the highest-contributing
    evidence appears first.  At most ``_EVIDENCE_LIMIT`` items per entity.
    """
    stmt = (
        select(Signal)
        .where(
            Signal.entity_id.isnot(None),
            Signal.score.isnot(None),
        )
        .order_by(
            Signal.entity_id,
            Signal.score.desc(),
            Signal.signal_date.desc().nulls_last(),
            Signal.created_at.desc(),
        )
    )
    evidence: dict[Any, list[dict]] = defaultdict(list)
    for row in db.scalars(stmt).all():
        bucket = evidence[row.entity_id]
        if len(bucket) < _EVIDENCE_LIMIT:
            bucket.append(
                {
                    "signal_type": row.signal_type,
                    "score": row.score,
                    "evidence": row.evidence,
                    "signal_date": str(row.signal_date) if row.signal_date else None,
                    "document_url": row.document_url,
                }
            )
    return dict(evidence)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def export_static_site(
    output_dir: str | Path,
    *,
    db: Session,
) -> dict[str, int]:
    """Export all scored data to a self-contained directory of static files.

    Reads from the ``entities``, ``alert_scores``, and ``signals`` tables and
    writes JSON data files plus HTML dashboard pages.  All JSON files are
    written atomically; HTML pages are written in-place (they are identical
    across runs).

    No ``db.commit()`` is called — this function is read-only with respect to
    the database.

    Parameters
    ----------
    output_dir:
        Destination directory path.  Created (including parents) if absent.
    db:
        SQLAlchemy session — used for reads only.

    Returns
    -------
    Summary dict with keys ``entities``, ``alerts``, ``files_written``.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "entities").mkdir(parents=True, exist_ok=True)

    logger.info("Starting static site export to %s", out)

    # ---- Fetch all data upfront (4 queries total; avoids N+1) ----
    all_entities: list[Entity] = list(db.scalars(select(Entity)).all())
    entity_map: dict[Any, Entity] = {e.id: e for e in all_entities}

    current_scores: list[AlertScore] = _fetch_current_scores(db)
    score_map: dict[Any, AlertScore] = {s.entity_id: s for s in current_scores}

    history_map = _fetch_score_history(db)
    evidence_map = _fetch_top_evidence(db)

    # ---- Sort alert scores: critical → elevated → watch → None, then date desc ----
    sorted_scores = sorted(
        current_scores,
        key=lambda s: (
            _LEVEL_ORDER.get(s.alert_level, 99),
            -(s.score_date.toordinal() if s.score_date else 0),
        ),
    )

    # Only emit records that have a non-None alert level in alerts.json
    alert_scores = [s for s in sorted_scores if s.alert_level is not None]

    # ---- Build top-level JSON payloads ----
    alerts_data: list[dict] = []
    for s in alert_scores:
        entity = entity_map.get(s.entity_id)
        alerts_data.append(
            {
                "entity_id": str(s.entity_id),
                "canonical_name": entity.canonical_name if entity else str(s.entity_id),
                "alert_level": s.alert_level,
                "composite_score": s.composite_score,
                "score_date": str(s.score_date),
                "component_scores": dict(s.component_scores or {}),
                "naics_code": entity.naics_code if entity else None,
            }
        )

    entities_data: list[dict] = []
    for e in all_entities:
        cur = score_map.get(e.id)
        entities_data.append(
            {
                "id": str(e.id),
                "canonical_name": e.canonical_name,
                "ticker": e.ticker,
                "naics_code": e.naics_code,
                "composite_score": cur.composite_score if cur else None,
                "alert_level": cur.alert_level if cur else None,
                "score_date": str(cur.score_date) if cur else None,
            }
        )

    meta_data: dict = {
        "exported_at": datetime.now(UTC).isoformat(),
        "entity_count": len(all_entities),
        "alert_count": len(alert_scores),
        "version": _VERSION,
    }

    # ---- Write JSON files ----
    files_written = 0

    _write_atomic(out / "meta.json", meta_data)
    files_written += 1
    _write_atomic(out / "alerts.json", alerts_data)
    files_written += 1
    _write_atomic(out / "entities.json", entities_data)
    files_written += 1

    for e in all_entities:
        cur = score_map.get(e.id)
        detail: dict = {
            "id": str(e.id),
            "canonical_name": e.canonical_name,
            "ticker": e.ticker,
            "naics_code": e.naics_code,
            "current_score": (
                {
                    "composite_score": cur.composite_score,
                    "alert_level": cur.alert_level,
                    "score_date": str(cur.score_date),
                    "component_scores": dict(cur.component_scores or {}),
                }
                if cur
                else None
            ),
            "score_history": history_map.get(e.id, []),
            "top_evidence": evidence_map.get(e.id, []),
        }
        _write_atomic(out / "entities" / f"{e.id}.json", detail)
        files_written += 1

    # ---- Write HTML dashboard pages ----
    for filename, content in HTML_PAGES.items():
        (out / filename).write_text(content, encoding="utf-8")
        files_written += 1

    logger.info(
        "Export complete: %d entities, %d alerts, %d files written.",
        len(all_entities),
        len(alert_scores),
        files_written,
    )
    return {
        "entities": len(all_entities),
        "alerts": len(alert_scores),
        "files_written": files_written,
    }


def export_digest(
    since_date: date,
    *,
    db: Session,
) -> str:
    """Return a plaintext weekly digest body.

    Summarises:

    - New critical/elevated alerts with ``score_date >= since_date``
    - Top 5 sectors (2-digit NAICS) ranked by average current composite score

    The caller is responsible for SMTP delivery — this function only produces
    the email body string.

    Parameters
    ----------
    since_date:
        Only alerts whose most-recent ``score_date`` falls on or after this
        date are included in the "new alerts" section.
    db:
        SQLAlchemy session — used for reads only.

    Returns
    -------
    Plaintext email body string.
    """
    # ---- Fetch current critical/elevated alerts since since_date ----
    subq = (
        select(
            AlertScore.entity_id,
            func.max(AlertScore.score_date).label("max_date"),
        )
        .group_by(AlertScore.entity_id)
        .subquery()
    )
    recent_scores: list[AlertScore] = list(
        db.scalars(
            select(AlertScore)
            .join(
                subq,
                (AlertScore.entity_id == subq.c.entity_id)
                & (AlertScore.score_date == subq.c.max_date),
            )
            .where(
                AlertScore.alert_level.in_(["critical", "elevated"]),
                AlertScore.score_date >= since_date,
            )
            .order_by(AlertScore.alert_level, AlertScore.score_date.desc())
        ).all()
    )

    # Fetch entity names for the scored entities
    alert_entity_ids = [s.entity_id for s in recent_scores]
    alert_entity_map: dict[Any, Entity] = {}
    if alert_entity_ids:
        for e in db.scalars(select(Entity).where(Entity.id.in_(alert_entity_ids))).all():
            alert_entity_map[e.id] = e

    # ---- Fetch all current scores for sector summary ----
    all_current = _fetch_current_scores(db)
    all_entity_map: dict[Any, Entity] = {e.id: e for e in db.scalars(select(Entity)).all()}

    naics_scores: dict[str, list[float]] = defaultdict(list)
    for s in all_current:
        entity = all_entity_map.get(s.entity_id)
        naics = ((entity.naics_code or "") + "")[:2] if entity else ""
        if naics:
            naics_scores[naics].append(s.composite_score or 0.0)

    # Top 5 sectors with at least 3 entities, by descending average score
    top_sectors = sorted(
        [
            (naics, sum(scores) / len(scores), len(scores))
            for naics, scores in naics_scores.items()
            if len(scores) >= 3
        ],
        key=lambda x: x[1],
        reverse=True,
    )[:5]

    # ---- Build plaintext digest ----
    sep = "=" * 60
    thin = "-" * 40
    lines = [
        sep,
        "Corporate Accountability Monitor \u2014 Weekly Digest",
        f"Period: {since_date} to {date.today()}",
        sep,
        "",
        f"NEW CRITICAL/ELEVATED ALERTS ({len(recent_scores)} entities)",
        thin,
    ]

    if not recent_scores:
        lines.append("  No new critical or elevated alerts this period.")
    else:
        for s in recent_scores:
            entity = alert_entity_map.get(s.entity_id)
            name = entity.canonical_name if entity else str(s.entity_id)
            lines.append(
                f"  [{s.alert_level.upper()}] {name}"
                f" \u2014 score {s.composite_score:.3f} ({s.score_date})"
            )

    lines += [
        "",
        "TOP SECTORS BY AVERAGE COMPOSITE SCORE",
        thin,
    ]

    if not top_sectors:
        lines.append(
            "  Insufficient data for sector comparison (need \u22653 entities per sector)."
        )
    else:
        for naics, avg, count in top_sectors:
            lines.append(f"  NAICS {naics}: avg score {avg:.3f} ({count} entities)")

    lines += [
        "",
        sep,
        "Generated by the Corporate Accountability Monitor.",
        "Do not reply to this message.",
        sep,
    ]

    return "\n".join(lines)
