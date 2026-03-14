"""
Unit tests for M10 — HSR Merger Screener.

Test cases cover:
- CVS/Aetna-equivalent high vertical-integration deal (score > 0.7)
- Horizontal deal in an unconcentrated market (score low)
- Conglomerate deal (score moderate)
- Prior-merger history incorporation via injectable lookup
- Score explainability: every triggered factor appears in output fields
- Edge cases: all factors, no factors, lookup exception handling
- Performance: scoring completes in < 100 ms
"""

from __future__ import annotations

import time
import uuid

from cam.analysis.merger_screener import (
    VERTICAL_RISK_FACTORS,
    MergerRiskScore,
    _collect_comparable_cases,
    _detect_text_factors,
    _generate_overlap_description,
    _generate_review_focus,
    score_merger,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

_ACQUIRER_ID = uuid.uuid4()


def _no_history(_entity_id: object) -> int:
    return 0


def _has_history(_entity_id: object) -> int:
    return 3  # three prior vertical acquisitions


# ---------------------------------------------------------------------------
# CVS/Aetna-equivalent: high vertical integration risk
# ---------------------------------------------------------------------------

CVS_AETNA_TARGET = (
    "Aetna Inc. is a managed care company offering health insurance plans, "
    "dental and disability benefits, and pharmacy benefit management services "
    "to over 22 million members across commercial and government programs."
)

CVS_AETNA_DEAL = (
    "CVS Health, which operates the Caremark pharmacy benefit manager (PBM) "
    "and a national retail pharmacy distribution network, seeks to acquire Aetna. "
    "Caremark functions as a critical input bottleneck: rival health insurers depend "
    "on CVS's PBM network for prescription-drug access and cannot easily switch to "
    "an alternative supplier. The combined entity will integrate insurance payer "
    "functions with PBM formulary and reimbursement rate-setting capabilities, "
    "creating a fully vertically integrated healthcare provider. The healthcare "
    "payer market is highly concentrated with an HHI exceeding 2,500 in multiple "
    "regional markets."
)


def test_score_merger_cvs_aetna_top_quartile():
    """CVS/Aetna-equivalent deal must score in the top quartile (> 0.7)."""
    result = score_merger(
        _ACQUIRER_ID,
        CVS_AETNA_TARGET,
        CVS_AETNA_DEAL,
        prior_merger_lookup=_has_history,
    )
    assert isinstance(result, MergerRiskScore)
    assert result.score > 0.7, f"Expected score > 0.7, got {result.score}"


def test_score_merger_cvs_aetna_factors_include_payer_provider():
    """CVS/Aetna deal must trigger payer_plus_provider."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert "payer_plus_provider" in result.risk_factors_present


def test_score_merger_cvs_aetna_factors_include_price_setter():
    """CVS/Aetna deal must trigger price_setter_plus_competitor (formulary / reimbursement)."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert "price_setter_plus_competitor" in result.risk_factors_present


def test_score_merger_cvs_aetna_factors_include_hhi():
    """CVS/Aetna deal must trigger high_hhi_either_market (explicit HHI mention)."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert "high_hhi_either_market" in result.risk_factors_present


def test_score_merger_cvs_aetna_prior_history_factor():
    """Prior-merger history lookup triggers prior_vertical_merger_same_firm."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert "prior_vertical_merger_same_firm" in result.risk_factors_present


def test_score_merger_cvs_aetna_comparable_cases_nonempty():
    """CVS/Aetna-equivalent deal must include comparable past case citations."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert len(result.comparable_past_cases) > 0


def test_score_merger_cvs_aetna_review_focus_nonempty():
    """High-risk deal must have a non-empty recommended_review_focus."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert result.recommended_review_focus
    assert len(result.recommended_review_focus) > 20


# ---------------------------------------------------------------------------
# Horizontal deal in an unconcentrated market: low risk
# ---------------------------------------------------------------------------

HORIZ_TARGET = (
    "Acme Office Supplies Co. sells paper, pens, and desk accessories through "
    "retail stores in the Midwest. The company has a 3% share of a fragmented "
    "stationery market with dozens of competitors."
)

HORIZ_DEAL = (
    "Generic Stationery LLC acquires Acme Office Supplies to expand its retail "
    "footprint in the Midwest. Both companies operate in the same product market "
    "at the same level of the supply chain. No upstream inputs are controlled by "
    "the acquirer; no healthcare, pharmacy, or platform services are involved."
)


def test_score_merger_horizontal_unconcentrated_low():
    """Horizontal deal in an unconcentrated market must score low (< 0.25)."""
    result = score_merger(_ACQUIRER_ID, HORIZ_TARGET, HORIZ_DEAL, prior_merger_lookup=_no_history)
    assert result.score < 0.25, f"Expected score < 0.25, got {result.score}"


def test_score_merger_horizontal_no_factors():
    """Horizontal unconcentrated deal should trigger zero risk factors."""
    result = score_merger(_ACQUIRER_ID, HORIZ_TARGET, HORIZ_DEAL, prior_merger_lookup=_no_history)
    assert result.risk_factors_present == []


def test_score_merger_horizontal_standard_review():
    """No-risk deal must produce a 'standard review' recommendation."""
    result = score_merger(_ACQUIRER_ID, HORIZ_TARGET, HORIZ_DEAL, prior_merger_lookup=_no_history)
    assert "standard review" in result.recommended_review_focus.lower()


# ---------------------------------------------------------------------------
# Conglomerate deal: moderate risk
# ---------------------------------------------------------------------------

CONG_TARGET = (
    "Vertex Logistics Corp. operates a nationwide trucking and freight-brokerage "
    "platform connecting shippers with independent carriers. The company holds a "
    "dominant market position in West Coast freight brokerage with pricing power "
    "over spot-rate transactions."
)

CONG_DEAL = (
    "MegaCorp Industries, a consumer-goods conglomerate, acquires Vertex Logistics "
    "to secure reliable distribution capacity. MegaCorp has no prior presence in "
    "freight brokerage. The target's freight-brokerage platform sets benchmark "
    "reference prices used by downstream shippers who also compete with MegaCorp "
    "in the consumer-goods retail channel."
)


def test_score_merger_conglomerate_moderate():
    """Conglomerate deal with partial vertical signals must score moderately (0.2–0.6)."""
    result = score_merger(_ACQUIRER_ID, CONG_TARGET, CONG_DEAL, prior_merger_lookup=_no_history)
    assert 0.2 <= result.score <= 0.6, f"Expected 0.2–0.6, got {result.score}"


def test_score_merger_conglomerate_has_some_factors():
    """Conglomerate deal triggers at least one risk factor."""
    result = score_merger(_ACQUIRER_ID, CONG_TARGET, CONG_DEAL, prior_merger_lookup=_no_history)
    assert len(result.risk_factors_present) >= 1


# ---------------------------------------------------------------------------
# Prior-merger history incorporation
# ---------------------------------------------------------------------------


def test_prior_merger_history_raises_score():
    """A deal with prior-merger history must score higher than without it."""
    result_no_history = score_merger(
        _ACQUIRER_ID,
        CONG_TARGET,
        CONG_DEAL,
        prior_merger_lookup=_no_history,
    )
    result_with_history = score_merger(
        _ACQUIRER_ID,
        CONG_TARGET,
        CONG_DEAL,
        prior_merger_lookup=_has_history,
    )
    assert result_with_history.score > result_no_history.score


def test_prior_merger_history_zero_count_not_added():
    """When lookup returns 0, prior_vertical_merger_same_firm must NOT be triggered."""
    result = score_merger(
        _ACQUIRER_ID,
        CONG_TARGET,
        CONG_DEAL,
        prior_merger_lookup=_no_history,
    )
    assert "prior_vertical_merger_same_firm" not in result.risk_factors_present


def test_prior_merger_history_nonzero_count_added():
    """When lookup returns > 0, prior_vertical_merger_same_firm must be triggered."""
    result = score_merger(
        _ACQUIRER_ID,
        CONG_TARGET,
        CONG_DEAL,
        prior_merger_lookup=_has_history,
    )
    assert "prior_vertical_merger_same_firm" in result.risk_factors_present


def test_prior_merger_lookup_exception_handled_gracefully():
    """Exception in prior_merger_lookup must not crash; history factor omitted."""

    def _raising_lookup(_entity_id: object) -> int:
        raise RuntimeError("DB is down")

    result = score_merger(
        _ACQUIRER_ID,
        CONG_TARGET,
        CONG_DEAL,
        prior_merger_lookup=_raising_lookup,
    )
    assert isinstance(result, MergerRiskScore)
    assert "prior_vertical_merger_same_firm" not in result.risk_factors_present


def test_no_prior_merger_lookup_omits_history_factor():
    """When prior_merger_lookup is not provided, prior history factor is absent."""
    result = score_merger(_ACQUIRER_ID, CONG_TARGET, CONG_DEAL)
    assert "prior_vertical_merger_same_firm" not in result.risk_factors_present


# ---------------------------------------------------------------------------
# Score explainability
# ---------------------------------------------------------------------------


def test_score_explainable_factors_in_overlap_description():
    """Every triggered factor should influence the market_overlap_description."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    # The description must be substantive (not the empty-factors fallback)
    assert "This transaction" in result.market_overlap_description


def test_score_explainable_comparable_cases_match_factors():
    """Each comparable case must correspond to at least one triggered factor."""
    from cam.analysis.merger_screener import _COMPARABLE_CASES

    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    all_factor_cases = {
        case for factor in result.risk_factors_present for case in _COMPARABLE_CASES.get(factor, [])
    }
    for case in result.comparable_past_cases:
        assert case in all_factor_cases, f"Comparable case not traceable to any factor: {case}"


def test_score_explainable_review_focus_nontrivial_for_high_risk():
    """High-risk deal review focus must mention a concrete remediation priority."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert "priority" in result.recommended_review_focus.lower()


# ---------------------------------------------------------------------------
# Score boundaries and arithmetic
# ---------------------------------------------------------------------------


def test_score_clamped_to_one():
    """Score must never exceed 1.0 even when all factors are triggered."""
    # All-keywords text guarantees all text-based factors fire
    all_keywords_text = (
        "bottleneck critical input sole supplier "
        "insurance payer health plan pharmacy benefit pbm "
        "marketplace third-party seller platform operator "
        "formulary reimbursement rate pricing power price setter "
        "highly concentrated hhi oligopoly"
    )
    result = score_merger(
        _ACQUIRER_ID,
        all_keywords_text,
        all_keywords_text,
        prior_merger_lookup=_has_history,
    )
    assert result.score <= 1.0


def test_score_zero_when_no_factors_and_no_history():
    """Completely generic text with no history must produce score == 0.0."""
    result = score_merger(
        _ACQUIRER_ID,
        "Widgets Inc. makes widgets.",
        "The buyer wants more widgets.",
        prior_merger_lookup=_no_history,
    )
    assert result.score == 0.0


def test_score_all_factors_trigger_near_max():
    """All factors triggered must yield score == 1.0 (max weight / max weight)."""
    all_keywords_text = (
        "bottleneck essential facility critical input gateway sole supplier "
        "insurance insurer health plan payer pharmacy benefit pbm "
        "marketplace ecommerce platform third-party seller merchant "
        "formulary reimbursement rate pricing power price setter "
        "highly concentrated hhi oligopoly"
    )
    result = score_merger(
        _ACQUIRER_ID,
        all_keywords_text,
        all_keywords_text,
        prior_merger_lookup=_has_history,
    )
    assert result.score == 1.0
    assert len(result.risk_factors_present) == len(VERTICAL_RISK_FACTORS)


def test_factors_ordered_by_weight_descending():
    """risk_factors_present must be sorted by factor weight, highest first."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    weights = [VERTICAL_RISK_FACTORS[f] for f in result.risk_factors_present]
    assert weights == sorted(weights, reverse=True)


def test_score_is_rounded_to_four_decimal_places():
    """Score must be rounded to 4 decimal places."""
    result = score_merger(
        _ACQUIRER_ID, CVS_AETNA_TARGET, CVS_AETNA_DEAL, prior_merger_lookup=_has_history
    )
    assert result.score == round(result.score, 4)


# ---------------------------------------------------------------------------
# Internal unit tests for helper functions
# ---------------------------------------------------------------------------


def test_detect_text_factors_bottleneck():
    detected = _detect_text_factors("The acquirer controls a bottleneck input.")
    assert "controls_bottleneck_input" in detected


def test_detect_text_factors_payer_provider():
    detected = _detect_text_factors("insurance payer and hospital network")
    assert "payer_plus_provider" in detected


def test_detect_text_factors_platform():
    detected = _detect_text_factors("ecommerce platform third-party seller")
    assert "platform_plus_seller" in detected


def test_detect_text_factors_price_setter():
    detected = _detect_text_factors("formulary and reimbursement rate setter")
    assert "price_setter_plus_competitor" in detected


def test_detect_text_factors_hhi():
    detected = _detect_text_factors("highly concentrated market with HHI above 3000")
    assert "high_hhi_either_market" in detected


def test_detect_text_factors_case_insensitive():
    detected = _detect_text_factors("BOTTLENECK INSURANCE MARKETPLACE FORMULARY HHI")
    assert "controls_bottleneck_input" in detected
    assert "payer_plus_provider" in detected
    assert "platform_plus_seller" in detected
    assert "price_setter_plus_competitor" in detected
    assert "high_hhi_either_market" in detected


def test_detect_text_factors_empty_text():
    assert _detect_text_factors("") == set()


def test_generate_overlap_description_empty_factors():
    desc = _generate_overlap_description([])
    assert "no vertical integration concerns" in desc.lower()


def test_generate_overlap_description_payer_provider():
    desc = _generate_overlap_description(["payer_plus_provider"])
    assert "insurance" in desc.lower() or "payer" in desc.lower()


def test_generate_review_focus_no_factors_standard():
    focus = _generate_review_focus([])
    assert "standard review" in focus.lower()


def test_generate_review_focus_bottleneck_priority():
    focus = _generate_review_focus(["controls_bottleneck_input", "high_hhi_either_market"])
    assert "priority" in focus.lower()
    assert (
        "foreclose" in focus.lower()
        or "bottleneck" in focus.lower()
        or "critical input" in focus.lower()
    )


def test_collect_comparable_cases_deduplication():
    """Duplicate cases across factors must appear only once."""
    # Trigger the same factor twice — dedup should apply
    cases = _collect_comparable_cases(["payer_plus_provider", "payer_plus_provider"])
    assert len(cases) == len(set(cases))


def test_collect_comparable_cases_empty_for_no_factors():
    assert _collect_comparable_cases([]) == []


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_score_merger_performance():
    """score_merger must complete within 100 ms."""
    start = time.perf_counter()
    score_merger(
        _ACQUIRER_ID,
        CVS_AETNA_TARGET,
        CVS_AETNA_DEAL,
        prior_merger_lookup=_has_history,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert elapsed_ms < 100, f"score_merger took {elapsed_ms:.1f} ms (limit 100 ms)"
