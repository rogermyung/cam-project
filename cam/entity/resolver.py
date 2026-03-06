"""
Entity resolution: map raw company name strings to canonical entity IDs.

Resolution pipeline (in order):
1. Exact match against entity_aliases table
2. Fuzzy match using token-based similarity (rapidfuzz) against all known aliases
3. External lookup via OpenCorporates / SEC EDGAR company search
4. Manual review queue for low-confidence matches

All thresholds are configurable via environment variables.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime

from rapidfuzz import fuzz, process
from sqlalchemy.orm import Session

from cam.db.models import Entity, EntityAlias, Signal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ResolveResult:
    entity_id: uuid.UUID | None
    canonical_name: str | None
    confidence: float  # 0.0 to 1.0
    method: str  # 'exact', 'fuzzy', 'api', 'unresolved'
    needs_review: bool
    raw_name: str = ""

    @property
    def resolved(self) -> bool:
        return self.entity_id is not None


@dataclass
class ReviewQueueItem:
    raw_name: str
    source: str
    confidence: float
    best_match_name: str | None
    best_match_entity_id: uuid.UUID | None
    created_at: datetime = field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# In-process review queue — used only in tests and same-process callers.
# Cross-process callers (worker → CLI) must use get_review_queue_from_db().
# ---------------------------------------------------------------------------

_review_queue: list[ReviewQueueItem] = []


def get_review_queue() -> list[ReviewQueueItem]:
    """Return items from the in-process queue (same-process / test use only)."""
    return list(_review_queue)


def get_review_queue_from_db(db: Session) -> list[ReviewQueueItem]:
    """
    Return review-queue items persisted in the Signal table.
    Use this in the CLI and any cross-process consumer.
    """
    signals = db.query(Signal).filter(Signal.signal_type == "entity_review_queue").all()
    items: list[ReviewQueueItem] = []
    for s in signals:
        evidence = json.loads(s.evidence or "{}")
        items.append(
            ReviewQueueItem(
                raw_name=evidence.get("raw_name", ""),
                source=s.source,
                confidence=s.score or 0.0,
                best_match_name=evidence.get("best_match_name"),
                best_match_entity_id=s.entity_id,
                created_at=s.created_at or datetime.utcnow(),
            )
        )
    return items


def clear_review_queue() -> None:
    """Clear the in-process review queue (used in tests)."""
    _review_queue.clear()


def resolve_review_item(raw_name: str, db: Session) -> bool:
    """Remove a review-queue item by raw name from both queues.

    Removes the matching item from the in-process queue and deletes the
    corresponding Signal row from the DB queue.

    Returns True if at least one item was found and removed.
    """
    # Remove from in-process queue
    before = len(_review_queue)
    _review_queue[:] = [item for item in _review_queue if item.raw_name != raw_name]
    removed_in_process = len(_review_queue) < before

    # Remove from DB queue
    removed_db = False
    signals = db.query(Signal).filter(Signal.signal_type == "entity_review_queue").all()
    for signal in signals:
        evidence = json.loads(signal.evidence or "{}")
        if evidence.get("raw_name") == raw_name:
            db.delete(signal)
            removed_db = True
    if removed_db:
        db.commit()

    return removed_in_process or removed_db


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

_STRIP_SUFFIXES = (
    " inc",
    " inc.",
    " incorporated",
    " corp",
    " corp.",
    " corporation",
    " llc",
    " l.l.c.",
    " ltd",
    " ltd.",
    " limited",
    " co",
    " co.",
    " company",
    " lp",
    " l.p.",
    " plc",
    " p.l.c.",
    " pllc",
    " group",
    " holdings",
    " holding",
    " international",
    " intl",
    " technologies",
    " technology",
    " tech",
    " services",
    " solutions",
    " enterprises",
    " partners",
)


def _normalize(name: str) -> str:
    """Lowercase, strip legal suffixes, collapse whitespace."""
    n = name.lower().strip()
    for suffix in _STRIP_SUFFIXES:
        if n.endswith(suffix):
            n = n[: -len(suffix)].rstrip(" ,.-")
    # Collapse internal whitespace and remove common punctuation
    n = " ".join(n.split())
    for ch in (".", ",", "-", "&", "'"):
        n = n.replace(ch, " ")
    n = " ".join(n.split())
    return n


# ---------------------------------------------------------------------------
# Core resolver
# ---------------------------------------------------------------------------


def resolve(
    raw_name: str,
    source: str,
    db: Session,
    hint: dict | None = None,
    fuzzy_threshold: float = 0.85,
    review_threshold: float = 0.65,
    external_lookup_fn=None,
) -> ResolveResult:
    """
    Resolve a raw company name to a canonical entity_id.

    Parameters
    ----------
    raw_name:            The raw string from the data source.
    source:              Identifier for the data source (e.g. 'osha').
    db:                  SQLAlchemy session.
    hint:                Optional dict with keys 'ticker', 'state', 'ein'.
    fuzzy_threshold:     Accept fuzzy match above this score automatically.
    review_threshold:    Queue for manual review above this score.
    external_lookup_fn:  Callable(raw_name, hint) -> ResolveResult | None.
                         Injected for testing; defaults to SEC EDGAR lookup.

    Returns
    -------
    ResolveResult
    """
    # ------------------------------------------------------------------
    # Step 1: exact match (normalised string)
    # ------------------------------------------------------------------
    normalised = _normalize(raw_name)
    alias = (
        db.query(EntityAlias)
        .filter(EntityAlias.raw_name == raw_name)
        .filter(EntityAlias.source == source)
        .first()
    )
    if alias:
        entity = db.get(Entity, alias.entity_id)
        return ResolveResult(
            entity_id=entity.id,
            canonical_name=entity.canonical_name,
            confidence=1.0,
            method="exact",
            needs_review=False,
            raw_name=raw_name,
        )

    # Also check normalised form against all aliases — pass source so we
    # prefer same-source aliases when multiple sources share a normalised name.
    normalised_alias = _exact_normalised_match(normalised, source, db)
    if normalised_alias:
        entity = db.get(Entity, normalised_alias.entity_id)
        # Persist alias for fast future lookups
        add_alias(entity.id, raw_name, source, 1.0, db)
        return ResolveResult(
            entity_id=entity.id,
            canonical_name=entity.canonical_name,
            confidence=1.0,
            method="exact",
            needs_review=False,
            raw_name=raw_name,
        )

    # ------------------------------------------------------------------
    # Step 2: fuzzy match against all known aliases
    #
    # Fetch source alongside raw_name so we can prefer same-source aliases
    # when multiple aliases score equally — mirroring bulk_resolve behaviour.
    # ------------------------------------------------------------------
    all_aliases = db.query(EntityAlias.raw_name, EntityAlias.entity_id, EntityAlias.source).all()
    if all_aliases:
        alias_names = [a.raw_name for a in all_aliases]
        alias_entity_ids = [a.entity_id for a in all_aliases]
        alias_sources = [a.source for a in all_aliases]
        alias_normalised = [_normalize(n) for n in alias_names]

        # Extract all matches at the best score so we can apply source preference.
        top_results = process.extract(
            normalised,
            alias_normalised,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
            limit=None,
        )
        if top_results:
            best_score_raw = top_results[0][1]
            # Collect all candidates tied at the best score
            top_candidates = [r for r in top_results if r[1] == best_score_raw]
            # Prefer same-source candidate; fall back to first (highest-score) match
            chosen = next(
                (r for r in top_candidates if alias_sources[r[2]] == source),
                top_results[0],
            )
            _, score_raw, idx = chosen
            score = score_raw / 100.0  # rapidfuzz returns 0-100

            if score >= fuzzy_threshold:
                entity_id = alias_entity_ids[idx]
                entity = db.get(Entity, entity_id)
                # Cache alias to speed up future exact lookups
                add_alias(entity_id, raw_name, source, score, db)
                return ResolveResult(
                    entity_id=entity.id,
                    canonical_name=entity.canonical_name,
                    confidence=score,
                    method="fuzzy",
                    needs_review=False,
                    raw_name=raw_name,
                )

            if score >= review_threshold:
                _queue_for_review(
                    raw_name,
                    source,
                    score,
                    alias_names[idx],
                    alias_entity_ids[idx],
                    db,
                )
                return ResolveResult(
                    entity_id=None,
                    canonical_name=None,
                    confidence=score,
                    method="unresolved",
                    needs_review=True,
                    raw_name=raw_name,
                )

    # ------------------------------------------------------------------
    # Step 3: external lookup (SEC EDGAR / OpenCorporates)
    # ------------------------------------------------------------------
    if external_lookup_fn is not None:
        ext_result = external_lookup_fn(raw_name, hint or {})
        if ext_result is not None and ext_result.resolved:
            add_alias(ext_result.entity_id, raw_name, source, ext_result.confidence, db)
            return ResolveResult(
                entity_id=ext_result.entity_id,
                canonical_name=ext_result.canonical_name,
                confidence=ext_result.confidence,
                method="api",
                needs_review=False,
                raw_name=raw_name,
            )

    # ------------------------------------------------------------------
    # Step 4: unresolved
    # ------------------------------------------------------------------
    logger.warning("Could not resolve entity for raw_name=%r source=%s", raw_name, source)
    return ResolveResult(
        entity_id=None,
        canonical_name=None,
        confidence=0.0,
        method="unresolved",
        needs_review=False,
        raw_name=raw_name,
    )


def _exact_normalised_match(normalised: str, source: str, db: Session) -> EntityAlias | None:
    """
    Return the best alias whose raw_name normalises to `normalised`.

    Prefers an alias from the same `source` — mirroring bulk_resolve behaviour
    and respecting the (raw_name, source) unique constraint which allows the same
    raw_name to point to *different* entities under different sources.  Falls back
    to the first match when no same-source alias exists.
    """
    all_aliases = db.query(EntityAlias).all()
    candidates = [a for a in all_aliases if _normalize(a.raw_name) == normalised]
    if not candidates:
        return None
    # Prefer same-source alias; fall back to the first candidate
    same_source = next((a for a in candidates if a.source == source), None)
    return same_source or candidates[0]


def _queue_for_review(
    raw_name: str,
    source: str,
    confidence: float,
    best_match_name: str | None,
    best_match_entity_id: uuid.UUID | None,
    db: Session,
) -> None:
    """
    Persist a review-queue item to the Signal table and to the in-process list.

    Flushes (but does not commit) so that the new row participates in the
    caller's transaction.  The caller is responsible for committing so that
    other processes (e.g. the CLI reading the queue from a separate DB
    connection) can see the rows.  bulk_resolve() issues a single commit after
    the loop; single-record callers of resolve() should commit themselves.
    """
    signal = Signal(
        entity_id=best_match_entity_id,
        source=source,
        signal_type="entity_review_queue",
        signal_date=date.today(),
        score=confidence,
        evidence=json.dumps({"raw_name": raw_name, "best_match_name": best_match_name}),
    )
    db.add(signal)
    db.flush()

    item = ReviewQueueItem(
        raw_name=raw_name,
        source=source,
        confidence=confidence,
        best_match_name=best_match_name,
        best_match_entity_id=best_match_entity_id,
    )
    _review_queue.append(item)
    logger.info(
        "Queued for manual review: %r (confidence=%.2f, best_match=%r)",
        raw_name,
        confidence,
        best_match_name,
    )


# ---------------------------------------------------------------------------
# Bulk resolution
# ---------------------------------------------------------------------------


def bulk_resolve(
    records: list[dict],
    source: str,
    db: Session,
    name_field: str = "name",
    hint_field: str | None = None,
    commit: bool = True,
    **kwargs,
) -> list[ResolveResult]:
    """
    Resolve a batch of records. Uses pre-loaded alias table to avoid
    N+1 queries; falls back to per-record resolution for fuzzy/API steps.

    Parameters
    ----------
    records:    List of dicts, each with at least `name_field`.
    source:     Data source identifier.
    db:         SQLAlchemy session.
    name_field: Key in each dict containing the raw company name.
    hint_field: Optional key containing a hint dict.
    commit:     If True (default), commit the session after the batch so that
                review-queue Signal rows are visible to other processes.  Pass
                False when the caller owns the transaction boundary (e.g.
                ingest_from_csv commits once after inserting events).
    **kwargs:   Passed through to resolve().
    """
    # Pre-load the full alias table once.
    # Exact map keyed by (raw_name, source) — mirrors the DB unique constraint
    # so we never silently pick the wrong entity when the same raw_name appears
    # under multiple sources.
    all_aliases_rows = db.query(EntityAlias).all()
    alias_map: dict[tuple[str, str], EntityAlias] = {
        (a.raw_name, a.source): a for a in all_aliases_rows
    }

    # Normalized map: norm → list of aliases; we prefer same-source matches.
    alias_norm_map: dict[str, list[EntityAlias]] = {}
    for a in all_aliases_rows:
        alias_norm_map.setdefault(_normalize(a.raw_name), []).append(a)

    results: list[ResolveResult] = []
    for record in records:
        raw_name = record.get(name_field, "")
        hint = record.get(hint_field) if hint_field else None

        # Fast-path: exact alias hit for this source
        if (raw_name, source) in alias_map:
            alias = alias_map[(raw_name, source)]
            entity = db.get(Entity, alias.entity_id)
            results.append(
                ResolveResult(
                    entity_id=entity.id,
                    canonical_name=entity.canonical_name,
                    confidence=1.0,
                    method="exact",
                    needs_review=False,
                    raw_name=raw_name,
                )
            )
            continue

        # Normalised exact-match — prefer same-source, fall back to first alias
        norm = _normalize(raw_name)
        if norm in alias_norm_map:
            candidates = alias_norm_map[norm]
            alias = next((a for a in candidates if a.source == source), candidates[0])
            entity = db.get(Entity, alias.entity_id)
            add_alias(entity.id, raw_name, source, 1.0, db)
            results.append(
                ResolveResult(
                    entity_id=entity.id,
                    canonical_name=entity.canonical_name,
                    confidence=1.0,
                    method="exact",
                    needs_review=False,
                    raw_name=raw_name,
                )
            )
            continue

        # Slow path: full resolve (fuzzy + optional API)
        result = resolve(raw_name, source, db, hint=hint, **kwargs)
        results.append(result)

    # Commit once for the whole batch so any review-queue Signal rows written
    # by _queue_for_review() become visible to other processes (e.g. the CLI).
    # Skipped when the caller sets commit=False to own the transaction boundary.
    if commit:
        db.commit()

    return results


# ---------------------------------------------------------------------------
# Alias management
# ---------------------------------------------------------------------------


def add_alias(
    entity_id: uuid.UUID,
    raw_name: str,
    source: str,
    confidence: float,
    db: Session,
) -> None:
    """
    Persist a new alias. Idempotent — silently skips if already exists
    (same raw_name + source).
    """
    existing = (
        db.query(EntityAlias)
        .filter(EntityAlias.raw_name == raw_name, EntityAlias.source == source)
        .first()
    )
    if existing:
        return

    alias = EntityAlias(
        id=uuid.uuid4(),
        entity_id=entity_id,
        raw_name=raw_name,
        source=source,
        confidence=confidence,
    )
    db.add(alias)
    db.flush()
