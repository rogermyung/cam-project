"""
Jev-backed entity alignment.

The resolver's fuzzy step compares normalised strings, which cannot bridge the
gap between how regulators name an employer and how the SEC names the filer:
``"SAFEWAY STORES 4680"`` and ``"Albertsons Companies, Inc."`` share no tokens,
so no ``token_sort_ratio`` threshold will ever link them.  That gap is why
WARN/OSHA/EPA/CFPB events land with ``entity_id=NULL``.

This module keeps rapidfuzz as a cheap candidate generator and asks TypeSafe's
Jev to adjudicate the shortlist — the decomposition from the
`entity alignment cookbook <https://docs.typesafe.ai/cookbooks/entity_alignment.md>`_:

* a per-candidate ``Score`` over three levels (different / related / same),
  whose rounded level *is* the decision — there is no threshold to fit;
* corroborating ``Noul`` questions that ride along as evidence and as a
  merge guard;
* one global ``Noul`` screening out raw names that are not companies at all.

All candidates' questions go out in a single request as speculative fan-out
(https://docs.typesafe.ai/patterns/fan-out.md): Jev ingests the state once and
evaluates every question against it in parallel, so the shortlist costs one
round trip rather than one per pair.  Code picks the winner and applies policy.

The model decides *nothing* about control flow.  It returns a level and some
probabilities; :func:`adjudicate` turns those into merge / review / reject by
explicit rules, and :func:`make_external_lookup` adapts that verdict to the
``external_lookup_fn`` contract in :mod:`cam.entity.resolver`.

Nothing here runs unless ``entity_jev_enabled`` is set, and the client is
injectable so tests never touch the network.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from rapidfuzz import fuzz, process
from sqlalchemy.orm import Session
from typesafe_sdk import (
    TypeSafeAuthenticationError,
    TypeSafeError,
    TypeSafePermissionDeniedError,
)

from cam.db.models import Entity
from cam.entity.resolver import (
    CANONICAL_SOURCE,
    ResolveResult,
    _load_candidates,
    _normalize,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

MERGE = "merge"
REVIEW = "review"
REJECT = "reject"

# Score levels, in the order sent to Jev.  The index is the level value, so
# ``round(score)`` indexes straight into this list.
SAME_ENTITY_LEVELS: list[dict[str, Any]] = [
    {
        "summary": "Different companies",
        "signals": [
            "The distinctive parts of the two names refer to unrelated businesses",
            "They share only a generic word such as National, American, First, or United",
            "One is a common-word coincidence rather than a shared brand",
        ],
    },
    {
        "summary": "Related companies, or too little to tell",
        "signals": [
            "Both belong to the same corporate family but neither clearly contains the other",
            "They could plausibly be the same company but the names are too generic to be sure",
            "One is a joint venture, franchisee, or licensee of the other rather than part of it",
        ],
    },
    {
        "summary": "The same company",
        "signals": [
            "The same company under a different legal suffix, spelling, or capitalisation",
            "A former or trading name of the candidate company",
            (
                "A subsidiary, division, brand, or individual facility of the candidate "
                "company — regulatory violations by a wholly owned unit are attributed "
                "to its parent"
            ),
        ],
    },
]

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AlignmentCandidate:
    """One registry entity offered to Jev as a possible match."""

    entity_id: uuid.UUID
    canonical_name: str
    ticker: str | None = None
    naics_code: str | None = None
    prefilter_score: float = 0.0  # rapidfuzz 0–1; kept for the audit trail only

    def as_state(self, key: str) -> dict[str, Any]:
        """Render for the request state.

        The rapidfuzz prefilter score is deliberately withheld: it is our own
        shortlisting artefact, not evidence about the two companies, and
        feeding it in would invite the model to ratify the string match we are
        trying to second-guess.
        """
        payload: dict[str, Any] = {"id": key, "canonical_name": self.canonical_name}
        if self.ticker:
            payload["ticker"] = self.ticker
        if self.naics_code:
            payload["naics_code"] = self.naics_code
        return payload


@dataclass
class AlignmentVerdict:
    """The adjudicated outcome for one raw name.

    ``verdict`` is the decision code owns: MERGE links the event to
    ``entity_id``, REVIEW sends it to the manual queue, REJECT leaves it
    unlinked.  ``level`` and ``rationale`` are the raw judgments, retained so a
    reviewer can see why — and so policy can be re-derived later without
    re-running inference.
    """

    verdict: str
    entity_id: uuid.UUID | None = None
    canonical_name: str | None = None
    confidence: float = 0.0
    level: float = 0.0  # raw Score position, 0.0–2.0
    rationale: dict[str, Any] = field(default_factory=dict)

    @property
    def resolved(self) -> bool:
        return self.verdict == MERGE and self.entity_id is not None


class _SystemOneClient(Protocol):
    """The one method this module needs, so tests can pass a stub."""

    def system_one(self, state: Any, questions: Any, **kwargs: Any) -> Any: ...


# ---------------------------------------------------------------------------
# Question construction
# ---------------------------------------------------------------------------

_RAW_NAME_NOTE = (
    "An employer name exactly as it appears in a US regulatory filing (WARN "
    "notice, OSHA inspection, EPA enforcement case, or CFPB complaint). It may "
    "be uppercased, abbreviated, truncated, misspelled, carry a store or "
    "facility number or a location suffix, or name a subsidiary, brand, or "
    "single site rather than the parent company."
)

_CANDIDATE_NOTE = (
    "A company in our reference registry, seeded from SEC filings, so its name "
    "is the parent filer's formal legal name."
)


def build_state(
    raw_name: str,
    source: str,
    candidates: Sequence[AlignmentCandidate],
    hint: dict | None = None,
) -> dict[str, Any]:
    """Assemble the request state: the raw name, its provenance, the shortlist."""
    state: dict[str, Any] = {
        "raw_name": raw_name,
        "raw_name_source": source,
        "candidates": [c.as_state(_key(i)) for i, c in enumerate(candidates)],
    }
    # Only include a hint when it carries something; an empty dict is noise.
    if hint:
        useful = {k: v for k, v in hint.items() if v not in (None, "")}
        if useful:
            state["raw_name_hint"] = useful
    return state


def _key(index: int) -> str:
    return f"c{index}"


def build_questions(candidates: Sequence[AlignmentCandidate]) -> dict[str, Any]:
    """Build one batch of questions covering every candidate.

    Per candidate: the three-level ``same_as_*`` Score that decides the case,
    plus two Nouls that are cheap, independently meaningful, and answerable
    from the names alone. Every candidate's questions are speculative — only
    the winner's answers are consumed — which is why they all travel together.
    """
    from typesafe_sdk import Noul, Score

    questions: dict[str, Any] = {
        # A global screen. WARN and OSHA extracts carry placeholder and
        # non-company rows ("Unknown", "Confidential", a named individual);
        # merging those into a real company is the expensive mistake.
        "raw_is_company": Noul(
            instructions={
                "question": "Does `raw_name` name a business or organisation?",
                "raw_name": _RAW_NAME_NOTE,
            },
            criteria={
                "true": "It names a company, non-profit, or other organisation.",
                "false": (
                    "It is a person's name, a government agency, or a placeholder "
                    "such as Unknown, N/A, Confidential, or Various."
                ),
            },
        )
    }

    for i, _candidate in enumerate(candidates):
        key = _key(i)
        path = f"candidates[{i}]"
        questions[f"same_as_{key}"] = Score(
            instructions={
                "question": (
                    f"Do `raw_name` and the `canonical_name` of `{path}` refer to the same company?"
                ),
                "raw_name": _RAW_NAME_NOTE,
                "candidate": _CANDIDATE_NOTE,
            },
            criteria=SAME_ENTITY_LEVELS,
        )
        questions[f"name_core_{key}"] = Noul(
            instructions={
                "question": (
                    f"Ignoring legal suffixes, punctuation, capitalisation, store or "
                    f"facility numbers, and location suffixes, is the distinctive part "
                    f"of `raw_name` the same distinctive name as the "
                    f"`canonical_name` of `{path}`?"
                ),
                "focus": (
                    "Judge the brand or family name itself, not whether the two are "
                    "the same legal entity."
                ),
            },
        )
        questions[f"subsidiary_{key}"] = Noul(
            instructions={
                "question": (
                    f"Is the business named by `raw_name` a subsidiary, division, "
                    f"brand, franchise, or individual facility of the company named "
                    f"by `{path}`?"
                ),
                "focus": (
                    "Answer no when they are the same top-level company under "
                    "different spellings, and no when they are unrelated."
                ),
            },
        )

    return questions


# ---------------------------------------------------------------------------
# Adjudication
# ---------------------------------------------------------------------------


def adjudicate(
    raw_name: str,
    source: str,
    candidates: Sequence[AlignmentCandidate],
    hint: dict | None = None,
    *,
    client: _SystemOneClient | None = None,
    model: str | None = None,
    merge_level: float | None = None,
    review_level: float | None = None,
) -> AlignmentVerdict:
    """Ask Jev which shortlisted candidate (if any) is the same company.

    Parameters
    ----------
    raw_name:      The raw employer string from the data source.
    source:        Data source identifier, included as provenance in the state.
    candidates:    Shortlist, typically rapidfuzz's top N.
    hint:          Optional dict with keys such as 'state', 'ticker', 'naics'.
    client:        Injected ``system_one`` caller. Built from settings when
                   omitted; tests always inject.
    model:         Jev model id; defaults to the ``jev_model`` setting.
    merge_level:   Score at or above which the match is auto-accepted.
    review_level:  Score at or above which the match goes to manual review.

    Returns
    -------
    :class:`AlignmentVerdict`. With no candidates, a REJECT verdict — the model
    is never asked a question it has no options for.

    Notes
    -----
    The default levels (1.5 / 0.5) are exactly the boundaries of
    ``round(score)``, so out of the box the decision is the cookbook's
    round-to-nearest-level rule with no fitted threshold. They are settings
    only so a deployment that has measured its own data can move them.
    """
    if not candidates:
        return AlignmentVerdict(verdict=REJECT, rationale={"reason": "no_candidates"})

    cfg = _levels(merge_level, review_level)
    resolved_client = client if client is not None else _default_client()

    state = build_state(raw_name, source, candidates, hint)
    questions = build_questions(candidates)

    kwargs: dict[str, Any] = {}
    if model is not None:
        kwargs["model"] = model
    response = resolved_client.system_one(state=state, questions=questions, **kwargs)

    return interpret(response, candidates, merge_level=cfg[0], review_level=cfg[1])


def interpret(
    response: Any,
    candidates: Sequence[AlignmentCandidate],
    *,
    merge_level: float,
    review_level: float,
) -> AlignmentVerdict:
    """Turn one batched response into a verdict.

    Separated from :func:`adjudicate` so the policy can be tested, replayed,
    and re-tuned against stored answers without issuing a request.
    """
    is_company = response.nouls["raw_is_company"].noul
    if is_company < 0.5:
        return AlignmentVerdict(
            verdict=REJECT,
            confidence=1.0 - is_company,
            rationale={"reason": "not_a_company", "raw_is_company": is_company},
        )

    # Pick the best candidate on its own merits. The per-candidate Scores are
    # independent judgments, so the winner is simply the highest level; ties
    # break toward the earlier candidate, which rapidfuzz ranked higher.
    levels = [response.scores[f"same_as_{_key(i)}"].score for i in range(len(candidates))]
    best = max(range(len(candidates)), key=lambda i: levels[i])
    best_key = _key(best)
    level = levels[best]
    winner = candidates[best]

    answer = response.scores[f"same_as_{best_key}"]
    name_core = response.nouls[f"name_core_{best_key}"].noul
    subsidiary = response.nouls[f"subsidiary_{best_key}"].noul

    rationale = {
        "raw_is_company": is_company,
        "level": level,
        "levels": {_key(i): lv for i, lv in enumerate(levels)},
        "name_core": name_core,
        "subsidiary": subsidiary,
        "prefilter_score": winner.prefilter_score,
        "candidate_name": winner.canonical_name,
    }

    if level >= merge_level:
        # Merge guard. A high level with no agreement on the distinctive name
        # is the shape of a plausible-sounding wrong merge, and an unwanted
        # merge is far more costly here than a duplicate: it attributes one
        # company's violations to another. Downgrade rather than guess.
        if name_core < 0.5 and subsidiary < 0.5:
            rationale["reason"] = "merge_guard_name_core"
            return AlignmentVerdict(
                verdict=REVIEW,
                entity_id=winner.entity_id,
                canonical_name=winner.canonical_name,
                confidence=answer.confidence,
                level=level,
                rationale=rationale,
            )
        return AlignmentVerdict(
            verdict=MERGE,
            entity_id=winner.entity_id,
            canonical_name=winner.canonical_name,
            confidence=answer.confidence,
            level=level,
            rationale=rationale,
        )

    if level >= review_level:
        return AlignmentVerdict(
            verdict=REVIEW,
            entity_id=winner.entity_id,
            canonical_name=winner.canonical_name,
            confidence=answer.confidence,
            level=level,
            rationale=rationale,
        )

    return AlignmentVerdict(
        verdict=REJECT,
        canonical_name=winner.canonical_name,
        confidence=answer.confidence,
        level=level,
        rationale=rationale,
    )


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------


def shortlist(
    raw_name: str,
    db: Session,
    limit: int = 5,
    *,
    min_prefilter: float = 0.0,
) -> list[AlignmentCandidate]:
    """Return the top *limit* registry entities for *raw_name*, best first.

    rapidfuzz does the cheap work of narrowing thousands of entities to a
    handful; Jev does the semantic work on the handful. Candidates are
    de-duplicated by entity, keeping each entity's best-scoring name, so a
    company with eight aliases cannot crowd out the rest of the shortlist.

    Scored with ``token_set_ratio``, deliberately *not* the
    ``token_sort_ratio`` the resolver uses to auto-accept. The two steps want
    opposite things: the resolver's threshold is a precision decision, so extra
    tokens should cost it ("Delta" must not accept "Delta Air Lines"), whereas
    a shortlist only needs recall, because Jev decides which candidate is
    actually right. ``token_set_ratio`` ignores tokens present in only one
    name, which recovers raw names carrying a location or facility suffix that
    ingestion did not strip — "STARBUCKS CORPORATION - SEATTLE WA" ranks
    nowhere under token_sort_ratio and first under token_set_ratio.
    """
    pool = _load_candidates(db)
    if not pool:
        return []

    normalised = _normalize(raw_name)
    matches = process.extract(
        normalised,
        [c.normalised for c in pool],
        scorer=fuzz.token_set_ratio,
        score_cutoff=min_prefilter * 100,
        limit=None,
    )

    best_per_entity: dict[uuid.UUID, float] = {}
    for _, score_raw, idx in matches:
        entity_id = pool[idx].entity_id
        score = score_raw / 100.0
        if score > best_per_entity.get(entity_id, -1.0):
            best_per_entity[entity_id] = score

    ranked = sorted(best_per_entity.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    if not ranked:
        return []

    entities = {
        e.id: e for e in db.query(Entity).filter(Entity.id.in_([eid for eid, _ in ranked])).all()
    }

    out: list[AlignmentCandidate] = []
    for entity_id, score in ranked:
        entity = entities.get(entity_id)
        if entity is None:  # pragma: no cover — alias pointing at a deleted entity
            continue
        out.append(
            AlignmentCandidate(
                entity_id=entity.id,
                canonical_name=entity.canonical_name,
                ticker=entity.ticker,
                naics_code=entity.naics_code,
                prefilter_score=score,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Resolver integration
# ---------------------------------------------------------------------------


def make_external_lookup(
    db: Session,
    source: str = "",
    *,
    client: _SystemOneClient | None = None,
    limit: int | None = None,
    model: str | None = None,
    merge_level: float | None = None,
    review_level: float | None = None,
):
    """Build an ``external_lookup_fn`` for :func:`cam.entity.resolver.resolve`.

    The returned callable has the signature the resolver expects,
    ``(raw_name, hint) -> ResolveResult | None``, and maps verdicts onto it:
    MERGE becomes a resolved result (the resolver records it as ``method='api'``
    and writes the alias), REVIEW becomes an unresolved result with
    ``needs_review=True`` (the resolver queues it), and REJECT becomes ``None``
    so the resolver reports a plain miss.

    *source* is the data source the batch is resolving for ('warn', 'osha', …).
    It travels in the state as provenance — a WARN employer string and a CFPB
    company string are written by different hands — and is bound here because
    the ``external_lookup_fn`` contract does not carry it per call.

    The client is created once per factory call, not per name, so a bulk run
    reuses one connection pool. A transient service error returns ``None`` and
    logs: a degraded Jev must leave the pipeline resolving exactly as it did
    before, never fail the ingest.

    Authentication and permission errors are the exception — they propagate.
    A rejected key is a misconfiguration that will reject every subsequent
    name too, and degrading quietly would report it as "Jev found no matches",
    which is indistinguishable from a working pipeline that resolved nothing.
    Silent zero-result runs are this project's most expensive failure mode, so
    a 401 fails the ingest loudly instead. Other exceptions propagate for the
    same reason: a bug in question construction is ours, not an outage.
    """
    from cam.config import get_settings

    settings = get_settings()
    resolved_limit = limit if limit is not None else settings.entity_jev_candidate_limit
    resolved_client = client if client is not None else _default_client()
    levels = _levels(merge_level, review_level)

    def lookup(raw_name: str, hint: dict | None = None) -> ResolveResult | None:
        candidates = shortlist(raw_name, db, limit=resolved_limit)
        if not candidates:
            return None

        try:
            verdict = adjudicate(
                raw_name,
                source,
                candidates,
                hint=hint,
                client=resolved_client,
                model=model,
                merge_level=levels[0],
                review_level=levels[1],
            )
        except (TypeSafeAuthenticationError, TypeSafePermissionDeniedError):
            # Never swallowed. A rejected credential fails every name in the
            # batch, and reporting that as "no match" is how a whole ingest run
            # ends up silently empty.
            logger.error(
                "Jev alignment rejected our credential; check TYPESAFE_API_KEY. "
                "Failing rather than resolving nothing silently."
            )
            raise
        except TypeSafeError as exc:
            # Rate limit, timeout, connection, 5xx — the transient half.
            # Ingestion must survive all of it with the pre-Jev behaviour
            # intact. Deliberately *not* a bare `Exception`: a malformed
            # question is our bug and should be loud too.
            logger.warning("Jev alignment unavailable for %r: %s", raw_name, exc)
            return None

        logger.info(
            "Jev alignment %r -> %s (%s, level=%.2f, confidence=%.2f)",
            raw_name,
            verdict.verdict,
            verdict.canonical_name,
            verdict.level,
            verdict.confidence,
        )

        if verdict.verdict == MERGE:
            return ResolveResult(
                entity_id=verdict.entity_id,
                canonical_name=verdict.canonical_name,
                confidence=verdict.confidence,
                method="api",
                needs_review=False,
                raw_name=raw_name,
            )
        if verdict.verdict == REVIEW:
            return ResolveResult(
                entity_id=None,
                canonical_name=verdict.canonical_name,
                confidence=verdict.confidence,
                method="unresolved",
                needs_review=True,
                raw_name=raw_name,
                # entity_id stays None so nothing links to an unreviewed match;
                # the candidate travels separately so the queue can name it.
                review_entity_id=verdict.entity_id,
            )
        return None

    return lookup


# ---------------------------------------------------------------------------
# Settings plumbing
# ---------------------------------------------------------------------------


def _levels(merge_level: float | None, review_level: float | None) -> tuple[float, float]:
    """Resolve the two decision levels, falling back to settings."""
    if merge_level is not None and review_level is not None:
        return merge_level, review_level
    from cam.config import get_settings

    settings = get_settings()
    return (
        merge_level if merge_level is not None else settings.entity_jev_merge_level,
        review_level if review_level is not None else settings.entity_jev_review_level,
    )


def _default_client() -> _SystemOneClient:
    """Build a TypeSafe client from settings.

    Raises rather than returning a stub when the key is missing: a silent
    no-op client would look like "Jev found no matches" and hide a
    misconfiguration behind an empty dashboard, which is the failure mode this
    whole branch exists to remove.
    """
    from typesafe_sdk import TypeSafeClient

    from cam.config import get_settings

    settings = get_settings()
    if not settings.typesafe_api_key:
        raise RuntimeError(
            "TYPESAFE_API_KEY is not set; cannot use Jev entity alignment. "
            "Set it in .env, or leave ENTITY_JEV_ENABLED unset to resolve "
            "with rapidfuzz only."
        )
    return TypeSafeClient(api_key=settings.typesafe_api_key, model=settings.jev_model)


__all__ = [
    "CANONICAL_SOURCE",
    "MERGE",
    "REJECT",
    "REVIEW",
    "SAME_ENTITY_LEVELS",
    "AlignmentCandidate",
    "AlignmentVerdict",
    "adjudicate",
    "build_questions",
    "build_state",
    "interpret",
    "make_external_lookup",
    "shortlist",
]
