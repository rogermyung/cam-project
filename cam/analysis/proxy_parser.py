"""
Proxy Statement Parser (M9).

Parses DEF 14A proxy filings to extract say-on-pay vote results, shareholder
proposals, executive compensation data, and escalating minority vote signals.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

# ---------------------------------------------------------------------------
# Proposal topic classification
# ---------------------------------------------------------------------------

PROPOSAL_TOPICS = [
    "worker_welfare",
    "environmental",
    "executive_pay",
    "supply_chain",
    "diversity",
    "political_spending",
    "other",
]

# Keyword lists per topic — order matters: first match wins.
# More specific topics (supply_chain, executive_pay) are listed before broader
# ones (worker_welfare) to prevent "labor" in worker_welfare from swallowing
# supply-chain-specific proposals.
_TOPIC_KEYWORDS: list[tuple[str, list[str]]] = [
    (
        "supply_chain",
        [
            "supply chain",
            "supplier",
            "forced labor",
            "child labor",
            "ilo",
            "tier-1",
            "tier-2",
            "human rights",
            "audit",
        ],
    ),
    (
        "executive_pay",
        [
            "executive compensation",
            "executive pay",
            "say-on-pay",
            "say on pay",
            "ceo pay",
            "compensation ratio",
            "pay ratio",
        ],
    ),
    (
        "environmental",
        [
            "greenhouse gas",
            "emissions",
            "climate",
            "carbon",
            "paris agreement",
            "scope 1",
            "scope 2",
            "scope 3",
            "environmental",
            "sustainability",
            "net zero",
        ],
    ),
    (
        "worker_welfare",
        [
            "worker",
            "workforce",
            "employee",
            "labor",
            "labour",
            "wage",
            "health and safety",
            "occupational",
            "turnover",
            "workplace injury",
        ],
    ),
    (
        "diversity",
        [
            "diversity",
            "equity",
            "inclusion",
            "gender",
            "racial",
            "ethnic",
            "dei",
            "equal opportunity",
            "representation",
        ],
    ),
    (
        "political_spending",
        [
            "political",
            "lobbying",
            "trade association",
            "campaign contribution",
            "pac",
            "political spending",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ProposalData:
    """Parsed shareholder proposal from a proxy statement."""

    topic: str  # classified topic
    proponent: str  # who filed it
    vote_for_pct: float
    vote_against_pct: float
    passed: bool
    management_recommendation: str  # 'FOR' or 'AGAINST'
    management_opposed: bool  # proponent and management on opposite sides


@dataclass
class ProxyData:
    """Structured data extracted from a DEF 14A proxy statement."""

    filing_date: date
    say_on_pay_pct: float | None  # % votes FOR executive compensation
    shareholder_proposals: list[ProposalData] = field(default_factory=list)
    executive_comp_total: float | None = None  # total CEO compensation USD
    median_worker_pay: float | None = None  # CEO pay ratio denominator
    ceo_pay_ratio: float | None = None


# ---------------------------------------------------------------------------
# Vote percentage extraction helpers
# ---------------------------------------------------------------------------

# Match vote percentage lines in common proxy formats:
#   "Votes For: 45,230,000 (58.3%)"  — count + parenthesised pct
#   "For: 58.3%"  or  "For | 58.3%"  — label + bare pct, colon or pipe separator
_PCT_LINE = re.compile(
    r"(?:votes?\s+)?(?P<label>for|against|abstain\w*)"
    r"[:\s|]+[\d,]+\s+\((?P<pct>[\d.]+)%\)"
    r"|"
    r"(?:votes?\s+)?(?P<label2>for|against)\s*[:\s|]+(?P<pct2>[\d.]+)%",
    re.IGNORECASE,
)

# Match bare percentage tables: "58.3%" on a line labelled 'for'
_PCT_BARE = re.compile(r"(?P<pct>[\d.]+)%", re.IGNORECASE)

# Match vote blocks for a proposal section
_VOTE_BLOCK = re.compile(
    r"vote\s+results?[:\s]*\n(.*?)(?=\n\s*\n|\Z)",
    re.IGNORECASE | re.DOTALL,
)

_MGMT_REC = re.compile(
    r"(?:board(?:\s+of\s+directors)?|management)\s+recommends?\s+a?\s*vote\s+"
    r"(?P<rec>FOR|AGAINST)",
    re.IGNORECASE,
)


def _parse_pct(text: str, label: str) -> float | None:
    """Extract percentage for 'for' or 'against' from a vote results block."""
    for m in _PCT_LINE.finditer(text):
        lbl = (m.group("label") or m.group("label2") or "").lower()
        pct_str = m.group("pct") or m.group("pct2")
        if lbl == label.lower() and pct_str:
            return float(pct_str)
    return None


def _parse_dollar(text: str) -> float | None:
    """Extract first dollar amount (e.g. '$24,750,000') as a float."""
    m = re.search(r"\$\s*([\d,]+)", text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


# ---------------------------------------------------------------------------
# Core parsing functions
# ---------------------------------------------------------------------------


def classify_proposal_topic(proposal_text: str) -> str:
    """Classify a shareholder proposal into a topic category.

    Parameters
    ----------
    proposal_text:
        Full text of the proposal (title + resolved clause).

    Returns
    -------
    One of :data:`PROPOSAL_TOPICS`; defaults to ``'other'``.
    """
    lower = proposal_text.lower()
    for topic, keywords in _TOPIC_KEYWORDS:
        if any(kw in lower for kw in keywords):
            return topic
    return "other"


def parse_proxy(filing_text: str, filing_date: date) -> ProxyData:
    """Parse a DEF 14A proxy filing into structured :class:`ProxyData`.

    Handles common proxy formats including tabular vote results with
    parenthesized percentages and plain percentage tables.

    Parameters
    ----------
    filing_text:
        Raw proxy filing text (plain text).
    filing_date:
        Date of the filing (typically the annual meeting date or filed date).

    Returns
    -------
    :class:`ProxyData` with all extractable fields populated.
    """
    result = ProxyData(filing_date=filing_date, say_on_pay_pct=None)

    # --- CEO total compensation ---
    ceo_match = re.search(
        r"chief\s+executive\s+officer\s*\|?\s*\$?\s*([\d,]+)",
        filing_text,
        re.IGNORECASE,
    )
    if ceo_match:
        result.executive_comp_total = float(ceo_match.group(1).replace(",", ""))

    # --- CEO pay ratio and median worker pay ---
    ratio_match = re.search(
        r"ratio\s+of\s+the\s+annual\s+total\s+compensation\s+of\s+our\s+ceo"
        r".*?was\s+(?P<ratio>[\d,]+)\s+to\s+1",
        filing_text,
        re.IGNORECASE | re.DOTALL,
    )
    if ratio_match:
        result.ceo_pay_ratio = float(ratio_match.group("ratio").replace(",", ""))

    median_match = re.search(
        r"median\s+annual\s+total\s+compensation\s+of\s+all\s+employees\s+was\s+"
        r"\$\s*([\d,]+)",
        filing_text,
        re.IGNORECASE,
    )
    if median_match:
        result.median_worker_pay = float(median_match.group(1).replace(",", ""))

    # --- Split filing into proposal sections ---
    # Each "Proposal N:" section becomes a block to parse independently
    proposal_blocks = re.split(
        r"(?=Proposal\s+\d+[:\.])",
        filing_text,
        flags=re.IGNORECASE,
    )

    for block in proposal_blocks:
        if not block.strip():
            continue

        # Determine management recommendation
        mgmt_m = _MGMT_REC.search(block)
        management_recommendation = mgmt_m.group("rec").upper() if mgmt_m else "FOR"

        # Extract vote percentages
        for_pct = _parse_pct(block, "for")
        against_pct = _parse_pct(block, "against")

        if for_pct is None and against_pct is None:
            continue  # no vote data in this block

        if for_pct is None:
            for_pct = 0.0
        if against_pct is None:
            against_pct = 0.0

        passed = for_pct > 50.0

        # --- Say-on-pay (advisory executive comp vote) ---
        # Identified by "say-on-pay" language OR "advisory vote" + "compensation"
        is_say_on_pay = bool(
            re.search(
                r"say.on.pay|advisory.*compensation|compensation.*advisory", block, re.IGNORECASE
            )
        )
        if is_say_on_pay and result.say_on_pay_pct is None:
            result.say_on_pay_pct = for_pct
            # Do NOT continue: the same block may also contain shareholder proposals
            # if the proposal splitter failed to separate them into distinct blocks.

        # --- Shareholder proposals ---
        # Identified by "RESOLVED" clause only — "Shareholder Proposals" section headers
        # appear in the say-on-pay block and must not trigger spurious proposal detection.
        is_shareholder = bool(re.search(r"\bRESOLVED\b", block, re.IGNORECASE))
        if not is_shareholder:
            continue

        # Extract proponent
        proponent = "Unknown"
        proponent_m = re.search(
            r"submitted\s+by[:\s]+(.+?)(?:\n|$)",
            block,
            re.IGNORECASE,
        )
        if proponent_m:
            proponent = proponent_m.group(1).strip()

        # Classify topic from the resolved clause + surrounding text
        resolved_m = re.search(r"RESOLVED.*?(?:\n\n|\Z)", block, re.IGNORECASE | re.DOTALL)
        topic_text = resolved_m.group(0) if resolved_m else block
        topic = classify_proposal_topic(topic_text)

        management_opposed = (
            management_recommendation == "AGAINST"
        )  # proponent filed FOR, mgmt recommends AGAINST

        result.shareholder_proposals.append(
            ProposalData(
                topic=topic,
                proponent=proponent,
                vote_for_pct=for_pct,
                vote_against_pct=against_pct,
                passed=passed,
                management_recommendation=management_recommendation,
                management_opposed=management_opposed,
            )
        )

    return result


def flag_escalating_minority(
    vote_series: list[float],
) -> bool:
    """Return True if a proposal's support has been consistently increasing.

    Parameters
    ----------
    vote_series:
        List of vote-FOR percentages ordered from oldest to most recent
        (e.g. ``[28.5, 36.7, 44.1]`` for a 3-year series).
        Must contain at least 2 data points.

    Returns
    -------
    True if every consecutive pair shows an increase (strict monotone increase).
    """
    if len(vote_series) < 2:
        return False
    return all(vote_series[i] < vote_series[i + 1] for i in range(len(vote_series) - 1))
