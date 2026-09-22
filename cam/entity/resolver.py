"""
Entity resolution: map raw company name strings to canonical entity IDs.

Resolution pipeline (in order):
1. Exact match against entity_aliases table
2. Normalised exact match against all alias rows *and* every
   Entity.canonical_name
3. Fuzzy match using token-based similarity (rapidfuzz) over the same pool
4. External lookup via OpenCorporates / SEC EDGAR company search
   (see cam.entity.jev_align for the Jev-backed implementation)
5. Manual review queue for low-confidence matches

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
    # The entity a needs_review result was *nearly* matched to.  Kept separate
    # from entity_id because entity_id being set means "link the event to this",
    # which is exactly what a review item must not do.  Carrying the candidate
    # anyway lets the review queue record it, so `cam.entity.cli accept` has a
    # UUID to work with instead of a name the reviewer must look up by hand.
    review_entity_id: uuid.UUID | None = None

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
# Resolution candidates
# ---------------------------------------------------------------------------

# Sentinel source for a candidate taken from Entity.canonical_name rather than
# from an entity_aliases row.  It can never equal a real data source, so the
# same-source tie-break in _pick_preferred() always prefers a genuine alias
# over a canonical name when both match equally well — preserving the
# behaviour that existed before canonical names joined the pool.
CANONICAL_SOURCE = "__canonical__"


@dataclass(frozen=True)
class _Candidate:
    """One name a raw string can be matched against.

    Candidates come from two places: rows in ``entity_aliases``, and each
    entity's own ``canonical_name``.  Including the canonical name matters —
    without it an entity is invisible to the resolver until some alias row
    happens to be written for it, so a raw name that exactly equalled an
    existing ``Entity.canonical_name`` resolved at confidence 0.00.
    """

    entity_id: uuid.UUID
    name: str
    source: str
    normalised: str


def _load_candidates(db: Session) -> list[_Candidate]:
    """Load every matchable name: alias rows first, then canonical names.

    Aliases come first so that a tie on match score resolves to an alias.
    Two column-only queries, no ORM hydration — cheaper than the two full
    ``db.query(EntityAlias).all()`` scans this replaced.
    """
    candidates: list[_Candidate] = [
        _Candidate(
            entity_id=row.entity_id,
            name=row.raw_name,
            source=row.source,
            normalised=_normalize(row.raw_name),
        )
        for row in db.query(EntityAlias.raw_name, EntityAlias.entity_id, EntityAlias.source).all()
    ]
    candidates.extend(
        _Candidate(
            entity_id=row.id,
            name=row.canonical_name,
            source=CANONICAL_SOURCE,
            normalised=_normalize(row.canonical_name),
        )
        for row in db.query(Entity.id, Entity.canonical_name).all()
    )
    return candidates


def _pick_preferred(candidates: list[_Candidate], source: str) -> _Candidate:
    """Return the same-source candidate if there is one, else the first.

    The (raw_name, source) unique constraint lets one normalised form map to
    two different entities under two different sources, so preferring the
    caller's own source is a correctness requirement, not a nicety.
    """
    return next((c for c in candidates if c.source == source), candidates[0])


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
                         A resolved result is accepted as method='api'; an
                         unresolved result with needs_review=True is queued
                         for manual review.  See
                         cam.entity.jev_align.make_external_lookup().

    Returns
    -------
    ResolveResult
    """
    # ------------------------------------------------------------------
    # Step 1: exact (raw_name, source) alias hit — the indexed fast path
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

    # ------------------------------------------------------------------
    # Step 2: normalised exact match over aliases *and* canonical names
    # ------------------------------------------------------------------
    # Load every matchable name once: alias rows plus entity canonical names.
    # Shared with the fuzzy step below, so steps 2 and 3 cost one scan between
    # them rather than one each.
    candidates = _load_candidates(db)

    # Normalised exact match over that pool — pass source so we prefer
    # same-source aliases when several candidates share a normalised name.
    normalised_matches = [c for c in candidates if c.normalised == normalised]
    if normalised_matches:
        chosen = _pick_preferred(normalised_matches, source)
        entity = db.get(Entity, chosen.entity_id)
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
    # Step 3: fuzzy match over the same candidate pool
    #
    # Candidates carry their source so we can prefer same-source matches when
    # several score equally — mirroring bulk_resolve behaviour.
    # ------------------------------------------------------------------
    if candidates:
        # Extract all matches at the best score so we can apply source preference.
        top_results = process.extract(
            normalised,
            [c.normalised for c in candidates],
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
            limit=None,
        )
        if top_results:
            best_score_raw = top_results[0][1]
            # Collect all candidates tied at the best score. process.extract()
            # returns them highest-first, so tied[0] is top_results[0].
            tied = [candidates[r[2]] for r in top_results if r[1] == best_score_raw]
            chosen = _pick_preferred(tied, source)
            score = best_score_raw / 100.0  # rapidfuzz returns 0-100

            if score >= fuzzy_threshold:
                entity = db.get(Entity, chosen.entity_id)
                # Cache alias to speed up future exact lookups
                add_alias(chosen.entity_id, raw_name, source, score, db)
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
                    chosen.name,
                    chosen.entity_id,
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
    # Step 4: external lookup (SEC EDGAR / OpenCorporates / Jev)
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
        # An unresolved-but-uncertain verdict must reach the review queue, the
        # same way the fuzzy step queues its middle band.  Without this an
        # external lookup that said "possibly the same company" would fall
        # through to step 5 and be recorded as a flat confidence-0.0 miss.
        if ext_result is not None and ext_result.needs_review:
            _queue_for_review(
                raw_name,
                source,
                ext_result.confidence,
                ext_result.canonical_name,
                ext_result.review_entity_id,
                db,
            )
            return ResolveResult(
                entity_id=None,
                canonical_name=ext_result.canonical_name,
                confidence=ext_result.confidence,
                method="unresolved",
                needs_review=True,
                raw_name=raw_name,
                review_entity_id=ext_result.review_entity_id,
            )

    # ------------------------------------------------------------------
    # Step 5: unresolved
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
# External lookup wiring
# ---------------------------------------------------------------------------


def default_external_lookup_fn(db: Session, source: str = ""):
    """Return the configured external lookup step, or None when disabled.

    Jev-backed alignment (``cam.entity.jev_align``) is the only external lookup
    implemented, and it is opt-in via ``entity_jev_enabled`` because it needs a
    credential and bills per token.  When it is off — the default — the
    resolver stops at the fuzzy step exactly as it always has.

    Imported lazily: jev_align imports this module, and settings may be absent
    in unit tests that only exercise the string-matching path.
    """
    try:
        from cam.config import get_settings

        settings = get_settings()
        enabled = settings.entity_jev_enabled
    except Exception:  # pragma: no cover — no settings in string-only tests
        return None

    if not enabled:
        return None

    from cam.entity.jev_align import make_external_lookup

    return make_external_lookup(db, source)


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
    **kwargs:   Passed through to resolve().  When ``external_lookup_fn`` is
                absent, the configured default is used (Jev alignment if
                ``entity_jev_enabled``, otherwise no external step); pass
                ``external_lookup_fn=None`` explicitly to suppress it.
    """
    # Resolve the external lookup once per batch, not once per record: the
    # factory builds an API client and this loop may run thousands of times.
    if "external_lookup_fn" not in kwargs:
        kwargs["external_lookup_fn"] = default_external_lookup_fn(db, source)
    # Pre-load every matchable name once: alias rows plus canonical names.
    candidates = _load_candidates(db)

    # Exact map keyed by (raw_name, source) — mirrors the DB unique constraint
    # so we never silently pick the wrong entity when the same raw_name appears
    # under multiple sources.  Canonical-name candidates are deliberately
    # excluded: their sentinel source can never equal a caller's source, so
    # they belong in the normalised map only.
    alias_map: dict[tuple[str, str], _Candidate] = {
        (c.name, c.source): c for c in candidates if c.source != CANONICAL_SOURCE
    }

    # Normalized map: norm → list of candidates; we prefer same-source matches.
    alias_norm_map: dict[str, list[_Candidate]] = {}
    for c in candidates:
        alias_norm_map.setdefault(c.normalised, []).append(c)

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

        # Normalised exact-match — prefer same-source, fall back to first candidate
        norm = _normalize(raw_name)
        if norm in alias_norm_map:
            alias = _pick_preferred(alias_norm_map[norm], source)
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
