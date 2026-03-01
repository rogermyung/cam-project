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

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from rapidfuzz import fuzz, process
from sqlalchemy.orm import Session

from cam.db.models import Entity, EntityAlias

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ResolveResult:
    entity_id: Optional[uuid.UUID]
    canonical_name: Optional[str]
    confidence: float          # 0.0 to 1.0
    method: str                # 'exact', 'fuzzy', 'api', 'unresolved'
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
    best_match_name: Optional[str]
    best_match_entity_id: Optional[uuid.UUID]
    created_at: datetime = field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# In-process review queue (backed by DB in production via Signal table)
# ---------------------------------------------------------------------------

_review_queue: list[ReviewQueueItem] = []


def get_review_queue() -> list[ReviewQueueItem]:
    """Return all items currently awaiting manual review."""
    return list(_review_queue)


def clear_review_queue() -> None:
    """Clear the in-process review queue (used in tests)."""
    _review_queue.clear()


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

_STRIP_SUFFIXES = (
    " inc", " inc.", " incorporated", " corp", " corp.", " corporation",
    " llc", " l.l.c.", " ltd", " ltd.", " limited", " co", " co.",
    " company", " lp", " l.p.", " plc", " p.l.c.", " pllc",
    " group", " holdings", " holding", " international", " intl",
    " technologies", " technology", " tech", " services", " solutions",
    " enterprises", " partners",
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
    hint: Optional[dict] = None,
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

    # Also check normalised form against all aliases
    normalised_alias = _exact_normalised_match(normalised, db)
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
    # ------------------------------------------------------------------
    all_aliases = db.query(EntityAlias.raw_name, EntityAlias.entity_id).all()
    if all_aliases:
        alias_names = [a.raw_name for a in all_aliases]
        alias_entity_ids = [a.entity_id for a in all_aliases]
        alias_normalised = [_normalize(n) for n in alias_names]

        result = process.extractOne(
            normalised,
            alias_normalised,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
        )
        if result:
            matched_normalised, score_raw, idx = result
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


def _exact_normalised_match(normalised: str, db: Session) -> Optional[EntityAlias]:
    """Check whether any alias normalises to the same string."""
    all_aliases = db.query(EntityAlias).all()
    for alias in all_aliases:
        if _normalize(alias.raw_name) == normalised:
            return alias
    return None


def _queue_for_review(
    raw_name: str,
    source: str,
    confidence: float,
    best_match_name: Optional[str],
    best_match_entity_id: Optional[uuid.UUID],
) -> None:
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
    hint_field: Optional[str] = None,
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
    **kwargs:   Passed through to resolve().
    """
    # Pre-load the full alias table once
    all_aliases_rows = db.query(EntityAlias).all()
    alias_map: dict[str, EntityAlias] = {a.raw_name: a for a in all_aliases_rows}
    alias_norm_map: dict[str, EntityAlias] = {
        _normalize(a.raw_name): a for a in all_aliases_rows
    }

    results: list[ResolveResult] = []
    for record in records:
        raw_name = record.get(name_field, "")
        hint = record.get(hint_field) if hint_field else None

        # Fast-path: exact alias hit
        if raw_name in alias_map:
            alias = alias_map[raw_name]
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

        # Normalised exact-match fast path
        norm = _normalize(raw_name)
        if norm in alias_norm_map:
            alias = alias_norm_map[norm]
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
        id=str(uuid.uuid4()),
        entity_id=entity_id,
        raw_name=raw_name,
        source=source,
        confidence=confidence,
    )
    db.add(alias)
    db.flush()
