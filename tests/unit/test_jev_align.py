"""
Tests for Jev-backed entity alignment (cam/entity/jev_align.py).

No live HTTP: every test injects a stub client that returns canned answers in
the SDK's own response shape, so the question construction, the decision
policy, and the resolver integration are all exercised offline.  The live
quality check against a labelled gold set lives in tests/smoke/test_jev_align.py.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typesafe_sdk import (
    TypeSafeAPIConnectionError,
    TypeSafeAuthenticationError,
    TypeSafeInternalServerError,
    TypeSafePermissionDeniedError,
    TypeSafeRateLimitError,
)

from cam.db.models import Base, Entity, EntityAlias
from cam.entity import jev_align
from cam.entity.jev_align import (
    MERGE,
    REJECT,
    REVIEW,
    AlignmentCandidate,
    adjudicate,
    build_questions,
    build_state,
    interpret,
    make_external_lookup,
    shortlist,
)
from cam.entity.resolver import (
    bulk_resolve,
    clear_review_queue,
    get_review_queue_from_db,
    resolve,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    session = sessionmaker(bind=eng)()
    yield session
    session.rollback()
    session.close()


@pytest.fixture(autouse=True)
def clean_queue():
    clear_review_queue()
    yield
    clear_review_queue()


def _candidate(name: str, ticker: str | None = None, prefilter: float = 0.8):
    return AlignmentCandidate(
        entity_id=uuid.uuid4(),
        canonical_name=name,
        ticker=ticker,
        prefilter_score=prefilter,
    )


# ---------------------------------------------------------------------------
# Stub client / response objects mirroring the typesafe_sdk shapes
# ---------------------------------------------------------------------------


class _Noul:
    def __init__(self, value: float):
        self.noul = value


class _Score:
    def __init__(self, value: float, confidence: float = 0.9):
        self.score = value
        self.confidence = confidence


class _Response:
    def __init__(self, nouls: dict, scores: dict):
        self.nouls = nouls
        self.scores = scores


def _api_error(cls, status: int):
    """Build an SDK API error; they carry the real HTTP triple, not a message."""
    import httpx2

    return cls(status=status, body=None, headers=httpx2.Headers())


class _StubClient:
    """Records the request it was given and replays a prepared response."""

    def __init__(self, response, error: Exception | None = None):
        self._response = response
        self._error = error
        self.calls: list[dict] = []

    def system_one(self, state, questions, **kwargs):
        self.calls.append({"state": state, "questions": questions, "kwargs": kwargs})
        if self._error is not None:
            raise self._error
        return self._response


def _response(
    levels: list[float],
    *,
    is_company: float = 0.98,
    name_core: list[float] | None = None,
    subsidiary: list[float] | None = None,
    confidence: float = 0.9,
) -> _Response:
    """Build a canned response for ``len(levels)`` candidates."""
    n = len(levels)
    name_core = name_core if name_core is not None else [0.95] * n
    subsidiary = subsidiary if subsidiary is not None else [0.05] * n
    nouls = {"raw_is_company": _Noul(is_company)}
    scores = {}
    for i in range(n):
        key = f"c{i}"
        scores[f"same_as_{key}"] = _Score(levels[i], confidence)
        nouls[f"name_core_{key}"] = _Noul(name_core[i])
        nouls[f"subsidiary_{key}"] = _Noul(subsidiary[i])
    return _Response(nouls, scores)


# ---------------------------------------------------------------------------
# State construction
# ---------------------------------------------------------------------------


class TestBuildState:
    def test_includes_raw_name_and_candidates(self):
        state = build_state("TYSON FOODS INC", "warn", [_candidate("Tyson Foods, Inc.", "TSN")])
        assert state["raw_name"] == "TYSON FOODS INC"
        assert state["raw_name_source"] == "warn"
        assert state["candidates"] == [
            {"id": "c0", "canonical_name": "Tyson Foods, Inc.", "ticker": "TSN"}
        ]

    def test_candidate_ids_are_positional(self):
        state = build_state("x", "osha", [_candidate("A"), _candidate("B"), _candidate("C")])
        assert [c["id"] for c in state["candidates"]] == ["c0", "c1", "c2"]

    def test_prefilter_score_is_withheld_from_the_model(self):
        state = build_state("x", "osha", [_candidate("A", prefilter=0.93)])
        assert "prefilter_score" not in state["candidates"][0]

    def test_empty_hint_omitted(self):
        assert "raw_name_hint" not in build_state("x", "osha", [_candidate("A")], hint={})

    def test_hint_included_when_useful(self):
        state = build_state("x", "osha", [_candidate("A")], hint={"state": "AR", "ein": None})
        assert state["raw_name_hint"] == {"state": "AR"}

    def test_hint_of_only_empty_values_omitted(self):
        state = build_state("x", "osha", [_candidate("A")], hint={"state": None, "ein": ""})
        assert "raw_name_hint" not in state


# ---------------------------------------------------------------------------
# Question construction
# ---------------------------------------------------------------------------


class TestBuildQuestions:
    def test_three_questions_per_candidate_plus_one_global(self):
        questions = build_questions([_candidate("A"), _candidate("B")])
        assert set(questions) == {
            "raw_is_company",
            "same_as_c0",
            "name_core_c0",
            "subsidiary_c0",
            "same_as_c1",
            "name_core_c1",
            "subsidiary_c1",
        }

    def test_same_as_is_a_three_level_score(self):
        q = build_questions([_candidate("A")])["same_as_c0"]
        assert q.type == "score"
        assert len(q.criteria) == 3

    def test_score_levels_are_ordered_different_to_same(self):
        levels = build_questions([_candidate("A")])["same_as_c0"].criteria
        assert "Different" in levels[0]["summary"]
        assert "same company" in levels[2]["summary"]

    def test_subsidiaries_count_as_the_same_company(self):
        """Violations by a wholly owned unit must roll up to the parent."""
        top = build_questions([_candidate("A")])["same_as_c0"].criteria[2]
        assert any("subsidiary" in s for s in top["signals"])

    def test_question_references_the_candidate_by_state_path(self):
        q = build_questions([_candidate("A"), _candidate("B")])["same_as_c1"]
        assert "`candidates[1]`" in q.instructions["question"]

    def test_global_screen_is_a_noul(self):
        assert build_questions([_candidate("A")])["raw_is_company"].type == "noul"

    def test_question_count_scales_linearly(self):
        for n in (1, 5, 10):
            assert len(build_questions([_candidate(str(i)) for i in range(n)])) == 3 * n + 1


# ---------------------------------------------------------------------------
# Decision policy
# ---------------------------------------------------------------------------


class TestInterpret:
    LEVELS = {"merge_level": 1.5, "review_level": 0.5}

    def test_top_level_merges(self):
        candidates = [_candidate("Tyson Foods, Inc.")]
        v = interpret(_response([2.0]), candidates, **self.LEVELS)
        assert v.verdict == MERGE
        assert v.entity_id == candidates[0].entity_id
        assert v.resolved

    def test_middle_level_goes_to_review(self):
        v = interpret(_response([1.0]), [_candidate("A")], **self.LEVELS)
        assert v.verdict == REVIEW
        assert not v.resolved

    def test_bottom_level_rejects(self):
        v = interpret(_response([0.0]), [_candidate("A")], **self.LEVELS)
        assert v.verdict == REJECT
        assert v.entity_id is None

    @pytest.mark.parametrize(
        "level,expected",
        [(0.0, REJECT), (0.49, REJECT), (0.5, REVIEW), (1.49, REVIEW), (1.5, MERGE), (2.0, MERGE)],
    )
    def test_default_levels_are_round_to_nearest(self, level, expected):
        """The 1.5/0.5 defaults reproduce round(score) — no fitted threshold."""
        v = interpret(_response([level]), [_candidate("A")], **self.LEVELS)
        assert v.verdict == expected

    def test_picks_the_highest_scoring_candidate(self):
        candidates = [_candidate("Wrong Co"), _candidate("Right Co"), _candidate("Also Wrong")]
        v = interpret(_response([0.2, 2.0, 0.4]), candidates, **self.LEVELS)
        assert v.entity_id == candidates[1].entity_id
        assert v.canonical_name == "Right Co"

    def test_ties_break_toward_the_better_prefiltered_candidate(self):
        candidates = [_candidate("First", prefilter=0.9), _candidate("Second", prefilter=0.7)]
        v = interpret(_response([2.0, 2.0]), candidates, **self.LEVELS)
        assert v.entity_id == candidates[0].entity_id

    def test_non_company_raw_name_rejected_before_any_candidate(self):
        v = interpret(_response([2.0], is_company=0.02), [_candidate("A")], **self.LEVELS)
        assert v.verdict == REJECT
        assert v.rationale["reason"] == "not_a_company"
        assert v.entity_id is None

    def test_merge_guard_downgrades_when_distinctive_name_disagrees(self):
        """High level but no shared distinctive name is the wrong-merge shape."""
        v = interpret(
            _response([2.0], name_core=[0.1], subsidiary=[0.1]),
            [_candidate("A")],
            **self.LEVELS,
        )
        assert v.verdict == REVIEW
        assert v.rationale["reason"] == "merge_guard_name_core"

    def test_merge_guard_allows_subsidiaries_with_different_names(self):
        """Tyson Fresh Meats -> Tyson Foods: different name, still the parent."""
        v = interpret(
            _response([2.0], name_core=[0.2], subsidiary=[0.95]),
            [_candidate("Tyson Foods, Inc.")],
            **self.LEVELS,
        )
        assert v.verdict == MERGE

    def test_rationale_retains_the_raw_judgments(self):
        v = interpret(_response([1.8, 0.3]), [_candidate("A"), _candidate("B")], **self.LEVELS)
        assert v.rationale["levels"] == {"c0": 1.8, "c1": 0.3}
        assert v.rationale["name_core"] == pytest.approx(0.95)
        assert v.rationale["subsidiary"] == pytest.approx(0.05)
        assert v.rationale["prefilter_score"] == pytest.approx(0.8)

    def test_confidence_comes_from_the_winning_score(self):
        v = interpret(_response([2.0], confidence=0.42), [_candidate("A")], **self.LEVELS)
        assert v.confidence == pytest.approx(0.42)

    def test_configured_levels_override_the_defaults(self):
        candidates = [_candidate("A")]
        strict = interpret(_response([1.6]), candidates, merge_level=1.9, review_level=0.5)
        assert strict.verdict == REVIEW


# ---------------------------------------------------------------------------
# adjudicate()
# ---------------------------------------------------------------------------


class TestAdjudicate:
    def test_sends_one_request_for_the_whole_shortlist(self):
        client = _StubClient(_response([0.1, 2.0, 0.3, 0.2, 0.1]))
        candidates = [_candidate(f"Co {i}") for i in range(5)]
        v = adjudicate(
            "SAFEWAY STORES 4680",
            "warn",
            candidates,
            client=client,
            merge_level=1.5,
            review_level=0.5,
        )
        assert len(client.calls) == 1, "shortlist must cost one round trip, not five"
        assert len(client.calls[0]["questions"]) == 16
        assert v.entity_id == candidates[1].entity_id

    def test_no_candidates_asks_nothing(self):
        client = _StubClient(_response([]))
        v = adjudicate("Unknown", "warn", [], client=client, merge_level=1.5, review_level=0.5)
        assert v.verdict == REJECT
        assert v.rationale["reason"] == "no_candidates"
        assert client.calls == []

    def test_model_passed_through_when_given(self):
        client = _StubClient(_response([2.0]))
        adjudicate(
            "x",
            "warn",
            [_candidate("A")],
            client=client,
            model="jev-1.13.0",
            merge_level=1.5,
            review_level=0.5,
        )
        assert client.calls[0]["kwargs"]["model"] == "jev-1.13.0"

    def test_model_omitted_when_not_given(self):
        client = _StubClient(_response([2.0]))
        adjudicate("x", "warn", [_candidate("A")], client=client, merge_level=1.5, review_level=0.5)
        assert "model" not in client.calls[0]["kwargs"]

    def test_no_live_http(self, monkeypatch):
        """A stub client must be used as-is; no real client is ever built."""

        def boom():
            raise AssertionError("_default_client() must not be called when client= is passed")

        monkeypatch.setattr(jev_align, "_default_client", boom)
        client = _StubClient(_response([2.0]))
        v = adjudicate(
            "x", "warn", [_candidate("A")], client=client, merge_level=1.5, review_level=0.5
        )
        assert v.verdict == MERGE


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------


class TestShortlist:
    def _seed(self, db, name: str, ticker: str | None = None, aliases: list[str] | None = None):
        entity = Entity(canonical_name=name, ticker=ticker)
        db.add(entity)
        db.flush()
        for alias in aliases or []:
            db.add(
                EntityAlias(
                    id=uuid.uuid4(),
                    entity_id=entity.id,
                    raw_name=alias,
                    source="sec_seed",
                    confidence=1.0,
                )
            )
        db.flush()
        return entity

    def test_empty_registry_returns_nothing(self, db):
        assert shortlist("Anything", db) == []

    def test_respects_the_limit(self, db):
        for i in range(12):
            self._seed(db, f"Acme Holdings {i}")
        assert len(shortlist("Acme Holdings", db, limit=5)) == 5

    def test_ranked_best_first(self, db):
        self._seed(db, "Tyson Foods, Inc.")
        self._seed(db, "Completely Unrelated Shipping")
        result = shortlist("Tyson Foods Inc", db, limit=2)
        assert result[0].canonical_name == "Tyson Foods, Inc."
        assert result[0].prefilter_score > result[1].prefilter_score

    def test_deduplicated_by_entity(self, db):
        """One entity with many aliases must not crowd out the shortlist."""
        self._seed(
            db,
            "Kroger Co",
            aliases=["KROGER CO", "Kroger Company", "THE KROGER CO", "Kroger"],
        )
        self._seed(db, "Krogen Yachts")
        result = shortlist("Kroger", db, limit=5)
        assert len({c.entity_id for c in result}) == len(result)
        assert len(result) == 2

    def test_carries_entity_metadata(self, db):
        self._seed(db, "Tyson Foods, Inc.", ticker="TSN")
        candidate = shortlist("Tyson Foods", db, limit=1)[0]
        assert candidate.ticker == "TSN"
        assert candidate.canonical_name == "Tyson Foods, Inc."

    def test_min_prefilter_drops_weak_candidates(self, db):
        self._seed(db, "Tyson Foods, Inc.")
        self._seed(db, "Zzz Unrelated Logistics")
        assert len(shortlist("Tyson Foods", db, limit=5, min_prefilter=0.6)) == 1


# ---------------------------------------------------------------------------
# Prefilter recall — the ceiling on everything downstream
# ---------------------------------------------------------------------------


class TestPrefilterRecall:
    """Measure how often rapidfuzz's shortlist contains the right answer.

    This is the hard ceiling on the whole approach: Jev can only choose among
    the candidates we hand it, so a name the prefilter never surfaces is a
    name no amount of adjudication will resolve.  Measured against a 245-entity
    slice of the registry, deliberately full of near-name distractors.
    """

    GOLD = Path(__file__).parent.parent / "fixtures" / "entity" / "alignment_gold.json"
    REGISTRY = Path(__file__).parent.parent / "fixtures" / "entity" / "registry_sample.json"

    # Measured, not aspirational. The two cases below this floor need a
    # subsidiary-to-parent mapping, which is a data problem, not a matcher one.
    MIN_RECALL_AT_5 = 0.83

    @pytest.fixture
    def registry(self, db):
        names = json.loads(self.REGISTRY.read_text())["entities"]
        for name in names:
            entity = Entity(canonical_name=name)
            db.add(entity)
            db.flush()
            db.add(
                EntityAlias(
                    id=uuid.uuid4(),
                    entity_id=entity.id,
                    raw_name=name,
                    source="sec_seed",
                    confidence=1.0,
                )
            )
        db.flush()
        return {name: name for name in names}

    def _merge_cases(self) -> list[dict]:
        cases = json.loads(self.GOLD.read_text())["cases"]
        return [c for c in cases if c["expected"] == MERGE]

    def test_recall_at_5_meets_the_measured_floor(self, db, registry):
        cases = self._merge_cases()
        misses = []
        for case in cases:
            target = case["candidates"][case["match_index"]]
            got = [c.canonical_name for c in shortlist(case["raw_name"], db, limit=5)]
            if target not in got:
                misses.append((case["raw_name"], target))

        recall = (len(cases) - len(misses)) / len(cases)
        assert recall >= self.MIN_RECALL_AT_5, (
            f"prefilter recall@5 {recall:.0%} below the {self.MIN_RECALL_AT_5:.0%} "
            f"floor; missed: {misses}"
        )

    def test_location_suffix_is_recovered(self, db, registry):
        """token_set_ratio's reason for being: an unstripped location suffix.

        Under token_sort_ratio this name ranked nowhere in the top 20 — the
        trailing " - SEATTLE WA" swamped the signal.
        """
        got = [
            c.canonical_name for c in shortlist("STARBUCKS CORPORATION - SEATTLE WA", db, limit=5)
        ]
        assert "Starbucks Corporation" in got

    def test_store_number_is_recovered(self, db, registry):
        got = [c.canonical_name for c in shortlist("CVS PHARMACY #10745", db, limit=5)]
        assert "CVS Health Corporation" in got

    def test_near_name_distractors_do_not_evict_the_answer(self, db, registry):
        """Three 'United' companies in the registry; the airline must surface."""
        got = [c.canonical_name for c in shortlist("UNITED AIRLINES", db, limit=5)]
        assert "United Airlines Holdings, Inc." in got

    def test_renamed_parent_is_a_known_miss(self, db, registry):
        """The documented ceiling, asserted so it cannot regress silently.

        "SAFEWAY STORES 4680" and "Albertsons Companies, Inc." share no tokens,
        so no string scorer can connect them. Closing this needs a subsidiary
        -> parent mapping (SEC Exhibit 21) feeding the shortlist, not a better
        threshold. If this test ever fails, the ceiling moved — raise
        MIN_RECALL_AT_5 and delete it.
        """
        got = [c.canonical_name for c in shortlist("SAFEWAY STORES 4680", db, limit=20)]
        assert "Albertsons Companies, Inc." not in got


# ---------------------------------------------------------------------------
# Resolver integration
# ---------------------------------------------------------------------------


class TestResolverIntegration:
    def _seed(self, db, name: str, ticker: str | None = None) -> Entity:
        entity = Entity(canonical_name=name, ticker=ticker)
        db.add(entity)
        db.flush()
        return entity

    def test_merge_verdict_resolves_through_the_resolver(self, db):
        """The case the whole branch exists for: no shared tokens, still linked."""
        entity = self._seed(db, "Albertsons Companies, Inc.", ticker="ACI")
        client = _StubClient(_response([2.0]))
        lookup = make_external_lookup(db, client=client, limit=5)

        result = resolve("SAFEWAY STORES 4680", "warn", db, external_lookup_fn=lookup)

        assert result.resolved
        assert result.entity_id == entity.id
        assert result.method == "api"

    def test_merge_writes_an_alias_so_the_next_hit_is_free(self, db):
        self._seed(db, "Albertsons Companies, Inc.")
        client = _StubClient(_response([2.0]))
        lookup = make_external_lookup(db, client=client, limit=5)

        resolve("SAFEWAY STORES 4680", "warn", db, external_lookup_fn=lookup)
        second = resolve("SAFEWAY STORES 4680", "warn", db, external_lookup_fn=lookup)

        assert second.method == "exact"
        assert len(client.calls) == 1, "second lookup must hit the alias cache"

    def test_review_verdict_reaches_the_review_queue(self, db):
        self._seed(db, "United Natural Foods, Inc.")
        client = _StubClient(_response([1.0]))
        lookup = make_external_lookup(db, client=client, limit=5)

        result = resolve("UNITED PROVISIONS LLC", "warn", db, external_lookup_fn=lookup)
        db.commit()

        assert not result.resolved
        assert result.needs_review
        queued = get_review_queue_from_db(db)
        assert [q.raw_name for q in queued] == ["UNITED PROVISIONS LLC"]
        assert queued[0].best_match_name == "United Natural Foods, Inc."

    def test_review_queue_records_the_candidate_entity_id(self, db):
        """`cam.entity.cli accept` needs a UUID, not just a name to look up."""
        entity = self._seed(db, "United Natural Foods, Inc.")
        client = _StubClient(_response([1.0]))
        lookup = make_external_lookup(db, "warn", client=client, limit=5)

        result = resolve("UNITED PROVISIONS LLC", "warn", db, external_lookup_fn=lookup)
        db.commit()

        assert result.review_entity_id == entity.id
        assert get_review_queue_from_db(db)[0].best_match_entity_id == entity.id

    def test_a_review_result_never_links_the_event(self, db):
        """entity_id must stay None, or ingestion would attach the event."""
        self._seed(db, "United Natural Foods, Inc.")
        client = _StubClient(_response([1.0]))
        lookup = make_external_lookup(db, "warn", client=client, limit=5)

        result = resolve("UNITED PROVISIONS LLC", "warn", db, external_lookup_fn=lookup)

        assert result.entity_id is None
        assert not result.resolved

    def test_reject_verdict_leaves_a_plain_miss(self, db):
        self._seed(db, "Tyson Foods, Inc.")
        client = _StubClient(_response([0.0]))
        lookup = make_external_lookup(db, client=client, limit=5)

        result = resolve("Springfield Nuclear Power Plant", "warn", db, external_lookup_fn=lookup)

        assert not result.resolved
        assert not result.needs_review
        assert result.confidence == 0.0

    @pytest.mark.parametrize(
        "make_error",
        [
            pytest.param(lambda: _api_error(TypeSafeRateLimitError, 429), id="rate_limit"),
            pytest.param(lambda: _api_error(TypeSafeInternalServerError, 503), id="server_error"),
            pytest.param(lambda: TypeSafeAPIConnectionError("connection reset"), id="connection"),
        ],
    )
    def test_service_failure_degrades_to_the_pre_jev_behaviour(self, db, make_error):
        """A rate limit or outage must not fail the ingest."""
        self._seed(db, "Tyson Foods, Inc.")
        client = _StubClient(None, error=make_error())
        lookup = make_external_lookup(db, client=client, limit=5)

        result = resolve("SAFEWAY STORES 4680", "warn", db, external_lookup_fn=lookup)

        assert not result.resolved
        assert result.method == "unresolved"

    @pytest.mark.parametrize(
        "make_error",
        [
            pytest.param(lambda: _api_error(TypeSafeAuthenticationError, 401), id="auth"),
            pytest.param(lambda: _api_error(TypeSafePermissionDeniedError, 403), id="forbidden"),
        ],
    )
    def test_a_rejected_credential_fails_loudly(self, db, make_error):
        """A bad key must not be reported as "Jev found no matches".

        It will reject every name in the batch, so degrading quietly turns a
        misconfiguration into a silently empty ingest — the failure mode this
        project has already been burned by twice.
        """
        self._seed(db, "Tyson Foods, Inc.")
        client = _StubClient(None, error=make_error())
        lookup = make_external_lookup(db, "warn", client=client, limit=5)

        with pytest.raises((TypeSafeAuthenticationError, TypeSafePermissionDeniedError)):
            lookup("SAFEWAY STORES 4680", {})

    def test_our_own_bugs_are_not_swallowed_as_outages(self, db):
        """A malformed question must be loud, not look like Jev being down."""
        self._seed(db, "Tyson Foods, Inc.")
        client = _StubClient(None, error=KeyError("same_as_c0"))
        lookup = make_external_lookup(db, client=client, limit=5)

        with pytest.raises(KeyError):
            lookup("SAFEWAY STORES 4680", {})

    def test_lookup_skipped_when_registry_is_empty(self, db):
        client = _StubClient(_response([2.0]))
        lookup = make_external_lookup(db, client=client, limit=5)
        assert lookup("Anything At All", {}) is None
        assert client.calls == []

    def test_data_source_travels_in_the_state(self, db):
        """Provenance matters: WARN and CFPB name companies differently."""
        self._seed(db, "Albertsons Companies, Inc.")
        client = _StubClient(_response([2.0]))
        lookup = make_external_lookup(db, "warn", client=client, limit=5)

        lookup("SAFEWAY STORES 4680", {})

        assert client.calls[0]["state"]["raw_name_source"] == "warn"

    def test_bulk_resolve_binds_its_own_source(self, db, monkeypatch):
        """bulk_resolve knows the source; external_lookup_fn's contract does not."""
        from cam.entity import resolver as resolver_mod

        self._seed(db, "Albertsons Companies, Inc.")
        client = _StubClient(_response([2.0]))
        seen: list[str] = []

        def fake_default(db_arg, source=""):
            seen.append(source)
            return make_external_lookup(db_arg, source, client=client, limit=5)

        monkeypatch.setattr(resolver_mod, "default_external_lookup_fn", fake_default)
        bulk_resolve([{"name": "SAFEWAY STORES 4680"}], "osha", db, commit=False)

        assert seen == ["osha"]
        assert client.calls[0]["state"]["raw_name_source"] == "osha"

    def test_exact_canonical_match_never_reaches_jev(self, db):
        """Step 1's canonical fix must short-circuit before any billed call."""
        self._seed(db, "Tyson Foods, Inc.")
        client = _StubClient(_response([2.0]))
        lookup = make_external_lookup(db, client=client, limit=5)

        result = resolve("TYSON FOODS INC", "warn", db, external_lookup_fn=lookup)

        assert result.method == "exact"
        assert client.calls == [], "string matching must run before paid inference"


# ---------------------------------------------------------------------------
# Configuration gate
# ---------------------------------------------------------------------------


class TestConfigGate:
    def test_disabled_by_default(self, db, monkeypatch):
        from cam.entity.resolver import default_external_lookup_fn

        monkeypatch.delenv("ENTITY_JEV_ENABLED", raising=False)
        assert default_external_lookup_fn(db) is None

    def test_enabled_requires_an_api_key(self, db, monkeypatch):
        from cam.entity.resolver import default_external_lookup_fn

        monkeypatch.setenv("ENTITY_JEV_ENABLED", "true")
        monkeypatch.setenv("TYPESAFE_API_KEY", "")
        with pytest.raises(RuntimeError, match="TYPESAFE_API_KEY"):
            default_external_lookup_fn(db)

    def test_enabled_with_a_key_builds_a_lookup(self, db, monkeypatch):
        from cam.entity.resolver import default_external_lookup_fn

        monkeypatch.setenv("ENTITY_JEV_ENABLED", "true")
        monkeypatch.setenv("TYPESAFE_API_KEY", "sk-test-not-a-real-key")
        assert callable(default_external_lookup_fn(db))
