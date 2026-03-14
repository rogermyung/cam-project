"""
HSR Merger Screener (M10).

Scores proposed merger transactions for vertical integration risk. Flags deals
where the acquirer controls a bottleneck input that the target's competitors
depend on, combining regulatory precedent with keyword-based market structure
analysis.

The ``score_merger`` function accepts an optional ``prior_merger_lookup``
callable so that unit tests can exercise the prior-history path without a
live database.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import UUID

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vertical integration risk factor weights (from PLAN.md)
# ---------------------------------------------------------------------------

VERTICAL_RISK_FACTORS: dict[str, float] = {
    # Acquirer already provides essential service to target's competitors
    "controls_bottleneck_input": 2.0,
    # Combines who pays with who provides (insurance + healthcare)
    "payer_plus_provider": 1.5,
    # Marketplace operator acquiring marketplace participant
    "platform_plus_seller": 1.5,
    # Entity that sets prices also competes at that price level
    "price_setter_plus_competitor": 2.0,
    # HHI > 2500 in either pre-merger market
    "high_hhi_either_market": 1.0,
    # Acquirer has made prior vertical acquisitions in same space
    "prior_vertical_merger_same_firm": 1.0,
}

# Sum of all weights — used to normalise the raw score to [0, 1]
_MAX_SCORE: float = sum(VERTICAL_RISK_FACTORS.values())  # 9.0

# ---------------------------------------------------------------------------
# Keyword detection tables
# ---------------------------------------------------------------------------

# Keys match VERTICAL_RISK_FACTORS.  Each list entry is a lowercase phrase;
# a case-insensitive substring match is sufficient for detection.
_FACTOR_KEYWORDS: dict[str, list[str]] = {
    "controls_bottleneck_input": [
        "bottleneck",
        "essential facility",
        "critical input",
        "gateway",
        "sole supplier",
        "must-have",
        "critical infrastructure",
        "distribution network",
        "captive customer",
        "locked in",
        "monopoly input",
        "last mile",
    ],
    "payer_plus_provider": [
        "insurance",
        "insurer",
        "health plan",
        "payer",
        "managed care",
        "pharmacy benefit",
        "pbm",
        "prescription drug",
        "healthcare provider",
        "hospital",
        "clinic",
        "physician",
        "medical group",
        "health maintenance",
        "hmo",
    ],
    "platform_plus_seller": [
        "marketplace",
        "ecommerce platform",
        "third-party seller",
        "merchant",
        "app store",
        "two-sided market",
        "platform participant",
        "platform operator",
        "exchange operator",
    ],
    "price_setter_plus_competitor": [
        "price setter",
        "benchmark price",
        "rate card",
        "reference price",
        "downstream competitor",
        "upstream price",
        "formulary",
        "reimbursement rate",
        "pricing power",
        "price maker",
    ],
    "high_hhi_either_market": [
        "hhi",
        "highly concentrated",
        "market concentration",
        "dominant market position",
        "market share above",
        "oligopoly",
        "duopoly",
        "near-monopoly",
    ],
}

# ---------------------------------------------------------------------------
# Comparable precedent case references
# ---------------------------------------------------------------------------

_COMPARABLE_CASES: dict[str, list[str]] = {
    "payer_plus_provider": [
        "FTC review of CVS/Aetna (2018) — consent decree on vertical PBM-insurer integration",
        "DOJ challenge to Aetna/Humana (2017) — blocked on payer market concentration",
        "FTC challenge to UnitedHealth/Change Healthcare (2022) — payer-IT vertical merger",
    ],
    "controls_bottleneck_input": [
        "United States v. AT&T/Time Warner (2018) — content/distribution bottleneck analysis",
        "FTC v. Illumina/Grail (2021) — genomic-sequencing platform acquiring downstream user",
        "DOJ challenge to Google/ITA Software (2011) — flight-data bottleneck concern",
    ],
    "platform_plus_seller": [
        "FTC v. Amazon (2023) — marketplace operator acquiring marketplace participant",
        "EU Commission review of Google/Fitbit (2021) — platform data-advantage theory",
    ],
    "price_setter_plus_competitor": [
        "FTC review of Surescripts (2019) — e-prescribing network as price setter",
        "DOJ v. United States Sugar/Imperial Sugar (2021) — price-setter horizontal analysis",
    ],
    "high_hhi_either_market": [
        "FTC v. Sysco/US Foods (2015) — highly concentrated food-distribution market",
        "DOJ v. JetBlue/Spirit (2023) — concentrated airline market remedies",
    ],
    "prior_vertical_merger_same_firm": [
        "FTC review of CVS/Aetna (2018) — cumulative vertical integration by CVS (post-Caremark)",
    ],
}

# Priority order for review-focus recommendation (highest-weight first)
_REVIEW_PRIORITY: list[str] = [
    "controls_bottleneck_input",
    "price_setter_plus_competitor",
    "payer_plus_provider",
    "platform_plus_seller",
    "prior_vertical_merger_same_firm",
    "high_hhi_either_market",
]

_REVIEW_FOCUS_TEXTS: dict[str, str] = {
    "controls_bottleneck_input": (
        "Priority: Assess whether the acquirer's control of a critical input will enable "
        "it to foreclose competitors from the target's downstream market. Request "
        "third-party dependency data and examine alternative supplier availability."
    ),
    "price_setter_plus_competitor": (
        "Priority: Examine whether post-merger the combined entity will have both the "
        "ability and incentive to discriminate against downstream rivals through pricing "
        "or reimbursement structures."
    ),
    "payer_plus_provider": (
        "Priority: Assess payer-provider integration risk — the combined entity may "
        "steer patients to in-network affiliates or disadvantage rival health plans "
        "relying on the same provider or pharmacy network."
    ),
    "platform_plus_seller": (
        "Priority: Review whether the marketplace operator will self-preference its "
        "newly acquired seller over independent merchants on the platform."
    ),
    "prior_vertical_merger_same_firm": (
        "Priority: Assess cumulative vertical integration effects. The acquirer's prior "
        "deals in this sector suggest a pattern of market foreclosure. Review all prior "
        "consent decrees for compliance and consider structural remedies."
    ),
    "high_hhi_either_market": (
        "Priority: Market concentration in at least one relevant market warrants detailed "
        "HHI analysis. Consider entry barriers, switching costs, and coordinated effects."
    ),
}

# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------


@dataclass
class MergerRiskScore:
    """Vertical integration risk assessment for a proposed merger transaction."""

    score: float  # 0.0 to 1.0
    risk_factors_present: list[str] = field(default_factory=list)
    market_overlap_description: str = ""
    comparable_past_cases: list[str] = field(default_factory=list)
    recommended_review_focus: str = ""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _detect_text_factors(combined_text: str) -> set[str]:
    """Return the set of VERTICAL_RISK_FACTORS keys detected via keyword matching."""
    lower = combined_text.lower()
    return {
        factor
        for factor, keywords in _FACTOR_KEYWORDS.items()
        if any(kw in lower for kw in keywords)
    }


def _generate_overlap_description(factors: list[str]) -> str:
    """Build a human-readable market-overlap description from triggered factors."""
    if not factors:
        return "No vertical integration concerns identified in the deal description."

    clauses: list[str] = []
    if "payer_plus_provider" in factors:
        clauses.append(
            "combines an insurance/payer entity with a healthcare provider or pharmacy-benefit manager"
        )
    if "controls_bottleneck_input" in factors:
        clauses.append(
            "acquirer controls a bottleneck input relied upon by competitors in the target's market"
        )
    if "platform_plus_seller" in factors:
        clauses.append("marketplace operator is acquiring a participant on its own platform")
    if "price_setter_plus_competitor" in factors:
        clauses.append(
            "acquirer sets prices or reimbursement rates at a level where the target competes"
        )
    if "high_hhi_either_market" in factors:
        clauses.append("one or both relevant markets exhibit high concentration (HHI > 2,500)")
    if "prior_vertical_merger_same_firm" in factors:
        clauses.append(
            "acquirer has a documented history of prior vertical acquisitions in this sector"
        )

    return "This transaction " + "; and ".join(clauses) + "."


def _generate_review_focus(factors: list[str]) -> str:
    """Return a plain-language regulatory review recommendation."""
    if not factors:
        return "Standard review; no vertical integration red flags detected."

    top_factor = next(
        (f for f in _REVIEW_PRIORITY if f in factors),
        factors[0],
    )
    return _REVIEW_FOCUS_TEXTS.get(
        top_factor,
        "Detailed vertical integration review recommended.",
    )


def _collect_comparable_cases(factors: list[str]) -> list[str]:
    """Collect precedent case citations for all triggered risk factors."""
    seen: set[str] = set()
    cases: list[str] = []
    for factor in factors:
        for case in _COMPARABLE_CASES.get(factor, []):
            if case not in seen:
                seen.add(case)
                cases.append(case)
    return cases


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def score_merger(
    acquirer_entity_id: UUID,
    target_description: str,
    deal_description: str,
    *,
    prior_merger_lookup: Callable[[UUID], int] | None = None,
) -> MergerRiskScore:
    """Score a proposed merger transaction for vertical integration risk.

    Args:
        acquirer_entity_id: UUID of the acquirer entity in the CAM entity table.
        target_description: Free-text description of the target company and its
            markets (e.g. from an FTC/DOJ press release).
        deal_description: Free-text description of the deal rationale and
            anticipated synergies.
        prior_merger_lookup: Optional callable ``(UUID) -> int`` returning the
            count of prior vertical acquisitions recorded in the database for the
            acquirer entity.  Pass a lambda in tests to avoid DB dependencies.
            Defaults to zero (no history) when omitted.

    Returns:
        :class:`MergerRiskScore` with:
        - ``score``: float in [0.0, 1.0] — ratio of triggered weight to max weight
        - ``risk_factors_present``: names of triggered factors, weight-descending
        - ``market_overlap_description``: auto-generated human-readable summary
        - ``comparable_past_cases``: citations of similar reviewed mergers
        - ``recommended_review_focus``: plain-language memo flag for reviewers
    """
    combined = f"{target_description} {deal_description}"

    # Text-based factor detection
    detected: set[str] = _detect_text_factors(combined)

    # Prior-merger history (injectable; defaults to no history)
    prior_count = 0
    if prior_merger_lookup is not None:
        try:
            prior_count = prior_merger_lookup(acquirer_entity_id)
        except Exception:
            logger.warning(
                "prior_merger_lookup raised an exception for entity %s; treating prior count as 0.",
                acquirer_entity_id,
            )

    if prior_count > 0:
        detected.add("prior_vertical_merger_same_firm")

    # Normalised score: raw weight sum / theoretical maximum
    raw_score = sum(VERTICAL_RISK_FACTORS[f] for f in detected)
    score = min(raw_score / _MAX_SCORE, 1.0)

    # Deterministic output: sort factors by weight descending, then name
    factors = sorted(
        detected,
        key=lambda f: (-VERTICAL_RISK_FACTORS[f], f),
    )

    return MergerRiskScore(
        score=round(score, 4),
        risk_factors_present=factors,
        market_overlap_description=_generate_overlap_description(factors),
        comparable_past_cases=_collect_comparable_cases(factors),
        recommended_review_focus=_generate_review_focus(factors),
    )
