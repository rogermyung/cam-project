"""
Proxy Statement Parser (M9).

Parses DEF 14A proxy filings to extract say-on-pay vote results, shareholder
proposals, executive compensation data, and escalating minority vote signals.

Topic classification has two implementations behind one seam. The default
walks :data:`_TOPIC_KEYWORDS` and takes the first list containing a matching
substring, which makes the result depend on the order of that list: a
supply-chain proposal mentioning "labor" would be filed under
``worker_welfare`` if the lists were reordered, and the comment on that table
exists to manage exactly this hazard. :func:`jev_topic_classifier` asks Jev a
single Choice over all seven topics instead, weighing the whole proposal
rather than racing keyword lists, and falls back to keywords if the service
is unavailable.

Every other field — vote percentages, dollar amounts, the management
recommendation, :func:`flag_escalating_minority` — stays regex and
arithmetic, because none of it needs semantic understanding.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date

logger = logging.getLogger(__name__)

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


# What each topic means, for the Choice. Written as what the proposal *asks
# the company to do*, since that is what distinguishes the overlapping cases:
# a proposal on supplier labour practices and one on the company's own
# workforce both talk about "labor", and only the target separates them.
_TOPIC_CRITERIA: dict[str, dict[str, object]] = {
    "supply_chain": {
        "what": (
            "Conditions in the company's supply chain: supplier labour "
            "practices, forced or child labour, human-rights due diligence, "
            "supplier audits, ILO standards."
        ),
        "not_for": "Conditions affecting the company's own direct employees.",
    },
    "worker_welfare": {
        "what": (
            "Treatment of the company's own workforce: wages, health and "
            "safety, freedom of association, turnover, workplace injuries, "
            "scheduling, classification."
        ),
        "not_for": ("Conditions at suppliers, and pay for named executives."),
    },
    "executive_pay": {
        "what": (
            "Executive or director compensation: say-on-pay, severance, "
            "clawbacks, performance metrics, the CEO-to-median-worker pay "
            "ratio."
        ),
        "not_for": "Pay for the general workforce, considered on its own.",
    },
    "environmental": {
        "what": (
            "Environmental impact: greenhouse gas emissions, climate risk and "
            "targets, pollution, water, waste, biodiversity, "
            "environmental-justice siting."
        ),
        "not_for": "Worker health and safety, even where the hazard is chemical.",
    },
    "diversity": {
        "what": (
            "Composition and equity of the workforce or board: diversity data, "
            "pay equity by gender or race, inclusion, equal-opportunity "
            "reporting."
        ),
        "not_for": "General workforce conditions with no equity dimension.",
    },
    "political_spending": {
        "what": (
            "Political and lobbying activity: campaign contributions, PAC "
            "spending, trade-association dues, lobbying disclosure, alignment "
            "of that spending with stated company policy."
        ),
        "not_for": "Regulatory compliance that involves no political spending.",
    },
    "other": {
        "what": (
            "Anything else, including governance mechanics such as written "
            "consent, special-meeting rights, board structure, auditor "
            "ratification, or share issuance."
        ),
        "not_for": "Any proposal that fits one of the topics above.",
    },
}


def build_topic_questions(proposal_texts: Sequence[str]) -> dict:
    """Build one Choice per proposal, all sharing a single state."""
    from typesafe_sdk import Choice

    return {
        f"topic_{i}": Choice(
            instructions={
                "question": (
                    f"What is the subject of the shareholder proposal at `proposals[{i}].text`?"
                ),
                "focus": (
                    "Judge what the proposal asks the company to do, not which "
                    "words it happens to use. Pick the single best fit; choose "
                    "'other' only when none of the named topics applies."
                ),
            },
            criteria=_TOPIC_CRITERIA,
        )
        for i in range(len(proposal_texts))
    }


def keyword_topic_classifier(proposal_texts: Sequence[str]) -> list[str]:
    """Substring classification, in the seam's shape. The default."""
    return [_classify_by_keyword(text) for text in proposal_texts]


def jev_topic_classifier(*, client=None, model: str | None = None):
    """Return a classifier that asks Jev instead of racing keyword lists.

    Every proposal in a filing travels in one request, so a proxy with eight
    proposals costs one round trip rather than eight.

    A transient service error falls back to keyword classification: a degraded
    Jev should cost accuracy, not availability. A rejected credential is
    re-raised, because it will reject every subsequent filing too and a silent
    fallback would hide the misconfiguration.
    """
    from cam import jev

    resolved_client = client if client is not None else jev.default_client(model=model)

    def classify(proposal_texts: Sequence[str]) -> list[str]:
        texts = list(proposal_texts)
        if not texts:
            return []

        state = {"proposals": [{"text": t} for t in texts]}
        try:
            response = jev.ask(
                state, build_topic_questions(texts), client=resolved_client, model=model
            )
        except jev.FATAL_ERRORS:
            logger.error(
                "Jev rejected our credential while classifying proposals; check "
                "TYPESAFE_API_KEY. Failing rather than silently downgrading to keywords."
            )
            raise
        except jev.TRANSIENT_ERRORS as exc:
            logger.warning("Jev unavailable for proposal topics (%s); using keywords.", exc)
            return keyword_topic_classifier(texts)

        # Guard the contract rather than trusting it: a topic outside
        # PROPOSAL_TOPICS would propagate into ProposalData.topic and from
        # there into the dashboard's topic facets.
        topics: list[str] = []
        for i in range(len(texts)):
            choice = response.choices[f"topic_{i}"].choice
            if choice not in PROPOSAL_TOPICS:
                logger.warning("Jev returned unknown topic %r; recording as 'other'.", choice)
                choice = "other"
            topics.append(choice)
        return topics

    return classify


def _classify_by_keyword(proposal_text: str) -> str:
    """First keyword list containing a match wins; hence the table's order."""
    lower = proposal_text.lower()
    for topic, keywords in _TOPIC_KEYWORDS:
        if any(kw in lower for kw in keywords):
            return topic
    return "other"


def _default_topic_classifier() -> Callable[[Sequence[str]], list[str]]:
    """Return the configured classifier: Jev when enabled, keywords otherwise."""
    from cam import jev

    if not jev.enabled("analysis_jev_enabled"):
        return keyword_topic_classifier
    return jev_topic_classifier()


def classify_proposal_topic(
    proposal_text: str,
    *,
    classifier: Callable[[Sequence[str]], list[str]] | None = None,
) -> str:
    """Classify a shareholder proposal into a topic category.

    Parameters
    ----------
    proposal_text:
        Full text of the proposal (title + resolved clause).
    classifier:
        Optional batched classifier ``(texts) -> topics``.  Defaults to
        substring matching, or :func:`jev_topic_classifier` when
        ``ANALYSIS_JEV_ENABLED`` is set.

    Returns
    -------
    One of :data:`PROPOSAL_TOPICS`; defaults to ``'other'``.

    Notes
    -----
    Classifying one proposal at a time costs one request each under the Jev
    classifier.  :func:`parse_proxy` batches a whole filing instead; prefer
    that, and reserve this for single lookups.
    """
    resolved = classifier if classifier is not None else _default_topic_classifier()
    return resolved([proposal_text])[0]


def parse_proxy(
    filing_text: str,
    filing_date: date,
    *,
    classifier: Callable[[Sequence[str]], list[str]] | None = None,
) -> ProxyData:
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
    # Proposal texts, collected in block order and classified as one batch
    # after the loop. Index i here always corresponds to
    # result.shareholder_proposals[i].
    topic_texts: list[str] = []

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

        # Collect the text now, classify the whole filing in one batch below.
        # Classifying inside this loop would cost one request per proposal.
        resolved_m = re.search(r"RESOLVED.*?(?:\n\n|\Z)", block, re.IGNORECASE | re.DOTALL)
        topic_texts.append(resolved_m.group(0) if resolved_m else block)

        management_opposed = (
            management_recommendation == "AGAINST"
        )  # proponent filed FOR, mgmt recommends AGAINST

        result.shareholder_proposals.append(
            ProposalData(
                # Placeholder; filled in by the batched classification below.
                topic="other",
                proponent=proponent,
                vote_for_pct=for_pct,
                vote_against_pct=against_pct,
                passed=passed,
                management_recommendation=management_recommendation,
                management_opposed=management_opposed,
            )
        )

    # One classification call for the whole filing.
    if topic_texts:
        resolved_classifier = classifier if classifier is not None else _default_topic_classifier()
        topics = resolved_classifier(topic_texts)
        if len(topics) != len(result.shareholder_proposals):
            # A classifier that loses or invents entries would silently
            # misalign topics against proposals, so refuse rather than zip.
            raise ValueError(
                f"classifier returned {len(topics)} topics for "
                f"{len(result.shareholder_proposals)} proposals"
            )
        for proposal, topic in zip(result.shareholder_proposals, topics):
            proposal.topic = topic

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
