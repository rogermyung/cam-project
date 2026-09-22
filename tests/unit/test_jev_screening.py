"""
Tests for Jev-backed screening (M9 proxy topics, M10 merger factors).

No live HTTP: every test injects a stub client returning canned answers in the
SDK's response shape. The live quality check against labelled fixtures lives
in tests/smoke/test_jev_screening.py.

The point of these tests is the seam, not the model: with Jev off, both
modules must behave exactly as they did before, and with Jev on they must
degrade back to keywords on a transient failure while failing loudly on a
rejected credential.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest
from typesafe_sdk import (
    TypeSafeAuthenticationError,
    TypeSafeInternalServerError,
    TypeSafePermissionDeniedError,
    TypeSafeRateLimitError,
)

from cam.analysis.merger_screener import (
    TEXT_FACTORS,
    VERTICAL_RISK_FACTORS,
    build_factor_questions,
    jev_factor_detector,
    keyword_detector,
    score_merger,
)
from cam.analysis.proxy_parser import (
    PROPOSAL_TOPICS,
    build_topic_questions,
    classify_proposal_topic,
    jev_topic_classifier,
    keyword_topic_classifier,
    parse_proxy,
)

# ---------------------------------------------------------------------------
# Stub client / response objects mirroring the typesafe_sdk shapes
# ---------------------------------------------------------------------------


class _Noul:
    def __init__(self, value: float):
        self.noul = value


class _Choice:
    def __init__(self, value: str, confidence: float = 0.9):
        self.choice = value
        self.confidence = confidence


class _Response:
    def __init__(self, nouls: dict | None = None, choices: dict | None = None):
        self.nouls = nouls or {}
        self.choices = choices or {}


def _api_error(cls, status: int):
    """Build an SDK API error; they carry the real HTTP triple, not a message."""
    import httpx2

    return cls(status=status, body=None, headers=httpx2.Headers())


class _StubClient:
    def __init__(self, response=None, error: Exception | None = None):
        self._response = response
        self._error = error
        self.calls: list[dict] = []

    def system_one(self, state, questions, **kwargs):
        self.calls.append({"state": state, "questions": questions, "kwargs": kwargs})
        if self._error is not None:
            raise self._error
        return self._response


def _factor_response(**probabilities: float) -> _Response:
    """Canned Noul answers; factors not named default to 0.0."""
    return _Response(nouls={f: _Noul(probabilities.get(f, 0.0)) for f in TEXT_FACTORS})


def _topic_response(*topics: str) -> _Response:
    return _Response(choices={f"topic_{i}": _Choice(t) for i, t in enumerate(topics)})


TRANSIENT = [
    pytest.param(lambda: _api_error(TypeSafeRateLimitError, 429), id="rate_limit"),
    pytest.param(lambda: _api_error(TypeSafeInternalServerError, 503), id="server_error"),
]
FATAL = [
    pytest.param(lambda: _api_error(TypeSafeAuthenticationError, 401), id="auth"),
    pytest.param(lambda: _api_error(TypeSafePermissionDeniedError, 403), id="forbidden"),
]


# ---------------------------------------------------------------------------
# M10 — merger risk factors
# ---------------------------------------------------------------------------


class TestMergerQuestions:
    def test_one_question_per_text_factor(self):
        questions = build_factor_questions()
        assert set(questions) == set(TEXT_FACTORS)

    def test_all_questions_are_nouls(self):
        assert all(q.type == "noul" for q in build_factor_questions().values())

    def test_prior_merger_factor_is_never_asked(self):
        """It comes from the database, not the text, so the model never sees it."""
        assert "prior_vertical_merger_same_firm" not in build_factor_questions()
        assert "prior_vertical_merger_same_firm" in VERTICAL_RISK_FACTORS

    def test_hhi_question_defers_the_arithmetic_to_code(self):
        q = build_factor_questions()["high_hhi_either_market"]
        assert "do not try to estimate" in q.instructions["focus"]


class TestMergerDetector:
    def _detect(self, client, target="t", deal="d", **kw):
        detector = jev_factor_detector(client=client, threshold=0.5, **kw)
        return detector(target, deal)

    def test_one_request_for_all_factors(self):
        client = _StubClient(_factor_response(payer_plus_provider=0.97))
        self._detect(client)
        assert len(client.calls) == 1
        assert len(client.calls[0]["questions"]) == len(TEXT_FACTORS)

    def test_probabilities_above_threshold_become_factors(self):
        client = _StubClient(
            _factor_response(payer_plus_provider=0.97, controls_bottleneck_input=0.88)
        )
        factors, _ = self._detect(client)
        assert factors == {"payer_plus_provider", "controls_bottleneck_input"}

    def test_probabilities_below_threshold_are_dropped(self):
        client = _StubClient(_factor_response(payer_plus_provider=0.49))
        factors, _ = self._detect(client)
        assert factors == set()

    def test_raw_probabilities_are_returned_for_the_audit_trail(self):
        client = _StubClient(_factor_response(payer_plus_provider=0.97))
        _, confidence = self._detect(client)
        assert confidence["payer_plus_provider"] == pytest.approx(0.97)
        assert set(confidence) == set(TEXT_FACTORS)

    def test_deal_text_reaches_the_model_as_separate_fields(self):
        client = _StubClient(_factor_response())
        self._detect(client, target="a hospital group", deal="synergies")
        state = client.calls[0]["state"]
        assert state["target_description"] == "a hospital group"
        assert state["deal_description"] == "synergies"

    def test_numeric_hhi_triggers_regardless_of_the_model(self):
        """HHI > 2500 is arithmetic; code owns it."""
        client = _StubClient(_factor_response(high_hhi_either_market=0.01))
        factors, _ = self._detect(client, deal="Pre-merger HHI of 3,100 in the served market.")
        assert "high_hhi_either_market" in factors

    def test_numeric_hhi_below_threshold_does_not_rescue_a_low_probability(self):
        client = _StubClient(_factor_response(high_hhi_either_market=0.01))
        factors, _ = self._detect(client, deal="Pre-merger HHI of 1,200.")
        assert "high_hhi_either_market" not in factors

    def test_numeric_hhi_survives_ordinary_regulatory_phrasing(self):
        """The window between "HHI" and its figure was too tight at 30 chars.

        "HHI in the relevant market is estimated at 3,400" puts 40 characters
        between the two and was silently missed — which matters more now that
        the model is explicitly told not to estimate HHI, leaving this the
        only path that can trigger the factor on a numeric value.
        """
        client = _StubClient(_factor_response(high_hhi_either_market=0.01))
        factors, _ = self._detect(
            client,
            deal="Post-merger HHI in the relevant market is estimated at 3,400.",
        )
        assert "high_hhi_either_market" in factors

    @pytest.mark.parametrize("make_error", TRANSIENT)
    def test_transient_failure_falls_back_to_keywords(self, make_error):
        client = _StubClient(error=make_error())
        detector = jev_factor_detector(client=client, threshold=0.5)
        target = "A regional health plan and its pharmacy benefit manager."

        factors, confidence = detector(target, "")

        assert factors == keyword_detector(target, "")[0]
        assert "payer_plus_provider" in factors
        assert confidence == {}, "the keyword fallback has no probabilities to report"

    @pytest.mark.parametrize("make_error", FATAL)
    def test_rejected_credential_fails_loudly(self, make_error):
        """A bad key rejects every deal; a quiet keyword fallback would hide it."""
        client = _StubClient(error=make_error())
        detector = jev_factor_detector(client=client, threshold=0.5)
        with pytest.raises((TypeSafeAuthenticationError, TypeSafePermissionDeniedError)):
            detector("t", "d")

    def test_threshold_is_configurable(self):
        client = _StubClient(_factor_response(payer_plus_provider=0.7))
        assert jev_factor_detector(client=client, threshold=0.6)("t", "d")[0]
        assert not jev_factor_detector(client=client, threshold=0.8)("t", "d")[0]


class TestScoreMergerSeam:
    def test_injected_detector_drives_the_score(self):
        def detector(_target, _deal):
            return {"payer_plus_provider"}, {"payer_plus_provider": 0.97}

        result = score_merger(uuid4(), "t", "d", detector=detector)

        assert result.risk_factors_present == ["payer_plus_provider"]
        assert result.score == pytest.approx(1.5 / 9.0, abs=1e-4)

    def test_confidence_is_carried_onto_the_result(self):
        def detector(_target, _deal):
            return {"payer_plus_provider"}, {"payer_plus_provider": 0.97}

        assert score_merger(uuid4(), "t", "d", detector=detector).factor_confidence == {
            "payer_plus_provider": 0.97
        }

    def test_keyword_detector_reports_no_confidence(self):
        result = score_merger(uuid4(), "An insurance company", "", detector=keyword_detector)
        assert result.factor_confidence == {}

    def test_score_still_comes_from_the_thresholded_set(self):
        """Probabilities ride along; they do not yet weight the score."""
        high = score_merger(
            uuid4(), "t", "d", detector=lambda _t, _d: ({"payer_plus_provider"}, {"x": 0.99})
        )
        low = score_merger(
            uuid4(), "t", "d", detector=lambda _t, _d: ({"payer_plus_provider"}, {"x": 0.51})
        )
        assert high.score == low.score

    def test_prior_merger_history_still_applies_over_any_detector(self):
        result = score_merger(
            uuid4(),
            "t",
            "d",
            prior_merger_lookup=lambda _e: 3,
            detector=lambda _t, _d: (set(), {}),
        )
        assert result.risk_factors_present == ["prior_vertical_merger_same_firm"]

    def test_precedent_and_review_text_are_unaffected_by_the_detector(self):
        result = score_merger(
            uuid4(), "t", "d", detector=lambda _t, _d: ({"payer_plus_provider"}, {})
        )
        assert any("CVS/Aetna" in c for c in result.comparable_past_cases)
        assert "payer-provider integration risk" in result.recommended_review_focus


# ---------------------------------------------------------------------------
# M9 — proxy proposal topics
# ---------------------------------------------------------------------------


class TestTopicQuestions:
    def test_one_choice_per_proposal(self):
        questions = build_topic_questions(["a", "b", "c"])
        assert set(questions) == {"topic_0", "topic_1", "topic_2"}
        assert all(q.type == "choice" for q in questions.values())

    def test_criteria_cover_every_topic(self):
        q = build_topic_questions(["a"])["topic_0"]
        assert set(q.criteria) == set(PROPOSAL_TOPICS)

    def test_question_references_its_proposal_by_state_path(self):
        q = build_topic_questions(["a", "b"])["topic_1"]
        assert "`proposals[1].text`" in q.instructions["question"]

    def test_supply_chain_and_worker_welfare_are_told_apart_by_target(self):
        """The overlap the keyword table manages by list ordering."""
        criteria = build_topic_questions(["a"])["topic_0"].criteria
        assert "own direct employees" in criteria["supply_chain"]["not_for"]
        assert "suppliers" in criteria["worker_welfare"]["not_for"]

    def test_no_questions_for_no_proposals(self):
        assert build_topic_questions([]) == {}


class TestTopicClassifier:
    def test_whole_filing_costs_one_request(self):
        client = _StubClient(_topic_response("environmental", "diversity", "executive_pay"))
        classifier = jev_topic_classifier(client=client)

        topics = classifier(["a", "b", "c"])

        assert topics == ["environmental", "diversity", "executive_pay"]
        assert len(client.calls) == 1, "one request per filing, not per proposal"

    def test_empty_input_asks_nothing(self):
        client = _StubClient(_topic_response())
        assert jev_topic_classifier(client=client)([]) == []
        assert client.calls == []

    def test_unknown_topic_is_coerced_to_other(self):
        """A topic outside PROPOSAL_TOPICS would leak into the dashboard facets."""
        client = _StubClient(_Response(choices={"topic_0": _Choice("cybersecurity")}))
        assert jev_topic_classifier(client=client)(["a"]) == ["other"]

    @pytest.mark.parametrize("make_error", TRANSIENT)
    def test_transient_failure_falls_back_to_keywords(self, make_error):
        client = _StubClient(error=make_error())
        text = "RESOLVED: report on greenhouse gas emissions and climate targets."
        assert jev_topic_classifier(client=client)([text]) == keyword_topic_classifier([text])

    @pytest.mark.parametrize("make_error", FATAL)
    def test_rejected_credential_fails_loudly(self, make_error):
        client = _StubClient(error=make_error())
        with pytest.raises((TypeSafeAuthenticationError, TypeSafePermissionDeniedError)):
            jev_topic_classifier(client=client)(["a"])

    def test_single_lookup_helper_uses_the_injected_classifier(self):
        client = _StubClient(_topic_response("political_spending"))
        topic = classify_proposal_topic("a", classifier=jev_topic_classifier(client=client))
        assert topic == "political_spending"


class TestParseProxySeam:
    FIXTURES = Path(__file__).parent.parent / "fixtures" / "edgar"

    def _proxy_text(self) -> str:
        return (self.FIXTURES / "proxy_escalating_minority.txt").read_text()

    def test_classifier_is_called_once_for_the_whole_filing(self):
        seen: list[list[str]] = []

        def classifier(texts):
            seen.append(list(texts))
            return ["environmental"] * len(texts)

        result = parse_proxy(self._proxy_text(), date(2024, 5, 1), classifier=classifier)

        assert len(seen) == 1, "one batched call, not one per proposal"
        assert len(seen[0]) == len(result.shareholder_proposals)

    def test_topics_are_assigned_in_proposal_order(self):
        def classifier(texts):
            return [f"t{i}" for i in range(len(texts))]

        result = parse_proxy(self._proxy_text(), date(2024, 5, 1), classifier=classifier)

        assert [p.topic for p in result.shareholder_proposals] == [
            f"t{i}" for i in range(len(result.shareholder_proposals))
        ]

    def test_miscounted_classifier_output_is_refused(self):
        """Zipping a short list would silently misalign topics against proposals."""
        with pytest.raises(ValueError, match="topics for"):
            parse_proxy(
                self._proxy_text(), date(2024, 5, 1), classifier=lambda _texts: ["environmental"]
            )

    def test_default_classifier_matches_the_pre_jev_behaviour(self):
        """With Jev off, parse_proxy must produce exactly what it always did."""
        text = self._proxy_text()
        explicit = parse_proxy(text, date(2024, 5, 1), classifier=keyword_topic_classifier)
        default = parse_proxy(text, date(2024, 5, 1))
        assert [p.topic for p in default.shareholder_proposals] == [
            p.topic for p in explicit.shareholder_proposals
        ]

    def test_no_proposals_needs_no_classifier(self):
        def boom(_texts):
            raise AssertionError("classifier must not run when there are no proposals")

        parse_proxy("No proposals here.", date(2024, 5, 1), classifier=boom)


# ---------------------------------------------------------------------------
# Configuration gate
# ---------------------------------------------------------------------------


class TestConfigGate:
    def test_disabled_by_default(self, monkeypatch):
        from cam.analysis.merger_screener import _default_detector
        from cam.analysis.proxy_parser import _default_topic_classifier

        monkeypatch.delenv("ANALYSIS_JEV_ENABLED", raising=False)
        assert _default_detector() is keyword_detector
        assert _default_topic_classifier() is keyword_topic_classifier

    def test_enabled_requires_an_api_key(self, monkeypatch):
        from cam.analysis.merger_screener import _default_detector

        monkeypatch.setenv("ANALYSIS_JEV_ENABLED", "true")
        monkeypatch.setenv("TYPESAFE_API_KEY", "")
        with pytest.raises(RuntimeError, match="TYPESAFE_API_KEY"):
            _default_detector()

    def test_enabled_with_a_key_builds_jev_backed_callables(self, monkeypatch):
        from cam.analysis.merger_screener import _default_detector
        from cam.analysis.proxy_parser import _default_topic_classifier

        monkeypatch.setenv("ANALYSIS_JEV_ENABLED", "true")
        monkeypatch.setenv("TYPESAFE_API_KEY", "sk-test-not-a-real-key")
        assert _default_detector() is not keyword_detector
        assert _default_topic_classifier() is not keyword_topic_classifier


# ---------------------------------------------------------------------------
# Labelled fixture sanity — measures the keyword baseline offline
# ---------------------------------------------------------------------------


class TestKeywordBaseline:
    """Score the existing keyword implementations against the gold set.

    This runs with no API key and no network. It exists so the live Jev
    comparison in tests/smoke/ has a baseline that is measured rather than
    remembered, and so a regression in the keyword path is caught here.
    """

    GOLD = Path(__file__).parent.parent / "fixtures" / "analysis" / "screening_gold.json"

    def test_topic_baseline_is_recorded_and_stable(self):
        cases = json.loads(self.GOLD.read_text())["proposal_topics"]
        got = keyword_topic_classifier([c["text"] for c in cases])
        correct = sum(g == c["expected"] for g, c in zip(got, cases))

        accuracy = correct / len(cases)
        # Measured at 60% (9/15) on 2026-09-21. Not aspirational: the keyword
        # classifier gets six of these wrong in known ways — see the `note` on
        # each. The bar is only that it does not get *worse*, so that a change
        # to _TOPIC_KEYWORDS or its ordering cannot quietly regress.
        assert accuracy >= 0.55, f"keyword topic accuracy fell to {accuracy:.0%}"

    def test_merger_baseline_is_recorded_and_stable(self):
        cases = json.loads(self.GOLD.read_text())["merger_factors"]
        correct = 0
        for case in cases:
            factors, _ = keyword_detector(case["target"], case["deal"])
            correct += factors == set(case["expected_factors"])

        accuracy = correct / len(cases)
        # Measured at 62% (5/8) on 2026-09-21, after widening the HHI window.
        # The three remaining misses are judgment failures substring matching
        # cannot fix: two false positives (negation, bare-word coincidence)
        # and one false negative (the economic theory stated without any
        # keyword from the list).
        assert accuracy >= 0.55, f"keyword factor accuracy fell to {accuracy:.0%}"
