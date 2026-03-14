"""
Unit tests for M9 — Proxy Statement Parser (cam/analysis/proxy_parser.py).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cam.analysis.proxy_parser import (
    PROPOSAL_TOPICS,
    ProposalData,
    ProxyData,
    classify_proposal_topic,
    flag_escalating_minority,
    parse_proxy,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "edgar"
FILING_DATE = date(2023, 5, 15)


# ---------------------------------------------------------------------------
# classify_proposal_topic
# ---------------------------------------------------------------------------


def test_classify_worker_welfare():
    text = "Report on worker health, safety, wages, and employee turnover rates."
    assert classify_proposal_topic(text) == "worker_welfare"


def test_classify_environmental():
    text = "Adopt science-based greenhouse gas emissions reduction targets aligned with Paris Agreement."
    assert classify_proposal_topic(text) == "environmental"


def test_classify_executive_pay():
    text = "Advisory vote to approve named executive officer compensation and CEO pay ratio."
    assert classify_proposal_topic(text) == "executive_pay"


def test_classify_supply_chain():
    text = "Adopt a supply chain labor standards policy requiring supplier audits for forced labor and child labor."
    assert classify_proposal_topic(text) == "supply_chain"


def test_classify_diversity():
    text = "Publish an annual diversity, equity, and inclusion report with gender and racial representation data."
    assert classify_proposal_topic(text) == "diversity"


def test_classify_political_spending():
    text = (
        "Disclose all political contributions, lobbying expenditures, and trade association dues."
    )
    assert classify_proposal_topic(text) == "political_spending"


def test_classify_other():
    text = (
        "This proposal concerns unrelated matters about corporate governance and board structure."
    )
    assert classify_proposal_topic(text) == "other"


def test_classify_returns_valid_topic():
    text = "Some random text without clear category signals."
    result = classify_proposal_topic(text)
    assert result in PROPOSAL_TOPICS


def test_classify_case_insensitive():
    text = "GREENHOUSE GAS EMISSIONS AND CLIMATE TARGETS"
    assert classify_proposal_topic(text) == "environmental"


# ---------------------------------------------------------------------------
# parse_proxy — failed say-on-pay fixture
# ---------------------------------------------------------------------------


def test_parse_failed_say_on_pay_returns_proxy_data():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert isinstance(result, ProxyData)
    assert result.filing_date == FILING_DATE


def test_parse_failed_say_on_pay_pct():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert result.say_on_pay_pct is not None
    assert abs(result.say_on_pay_pct - 58.3) < 0.1


def test_parse_failed_say_on_pay_below_70():
    """Say-on-pay below 70% is a signal — verify it parses as below threshold."""
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert result.say_on_pay_pct < 70.0


def test_parse_failed_say_on_pay_ceo_comp():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert result.executive_comp_total is not None
    assert result.executive_comp_total == pytest.approx(24_750_000, rel=0.01)


def test_parse_failed_say_on_pay_median_worker():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert result.median_worker_pay is not None
    assert result.median_worker_pay == pytest.approx(79_327, rel=0.01)


def test_parse_failed_say_on_pay_ceo_ratio():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert result.ceo_pay_ratio is not None
    assert result.ceo_pay_ratio == pytest.approx(312.0, rel=0.01)


def test_parse_failed_say_on_pay_proposals():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    assert len(result.shareholder_proposals) >= 2


def test_parse_failed_say_on_pay_management_opposed():
    """Worker welfare and political spending proposals — board opposes both."""
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    opposed = [p for p in result.shareholder_proposals if p.management_opposed]
    assert len(opposed) >= 1


def test_parse_failed_say_on_pay_proposal_topics():
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    topics = {p.topic for p in result.shareholder_proposals}
    assert "worker_welfare" in topics or "political_spending" in topics


def test_parse_failed_say_on_pay_proposal_structure():
    """Each ProposalData must have all required fields."""
    text = (FIXTURES / "proxy_failed_say_on_pay.txt").read_text()
    result = parse_proxy(text, FILING_DATE)
    for p in result.shareholder_proposals:
        assert isinstance(p, ProposalData)
        assert 0.0 <= p.vote_for_pct <= 100.0
        assert 0.0 <= p.vote_against_pct <= 100.0
        assert isinstance(p.passed, bool)
        assert p.management_recommendation in ("FOR", "AGAINST")
        assert p.topic in PROPOSAL_TOPICS


# ---------------------------------------------------------------------------
# parse_proxy — escalating minority fixture
# ---------------------------------------------------------------------------


def test_parse_escalating_minority_say_on_pay():
    text = (FIXTURES / "proxy_escalating_minority.txt").read_text()
    result = parse_proxy(text, date(2023, 6, 8))
    assert result.say_on_pay_pct is not None
    assert result.say_on_pay_pct > 70.0  # passes comfortably


def test_parse_escalating_minority_proposals():
    text = (FIXTURES / "proxy_escalating_minority.txt").read_text()
    result = parse_proxy(text, date(2023, 6, 8))
    assert len(result.shareholder_proposals) >= 1


def test_parse_escalating_minority_supply_chain_topic():
    text = (FIXTURES / "proxy_escalating_minority.txt").read_text()
    result = parse_proxy(text, date(2023, 6, 8))
    topics = {p.topic for p in result.shareholder_proposals}
    assert "supply_chain" in topics or "worker_welfare" in topics


def test_parse_escalating_minority_vote_for_pct():
    """The supply chain proposal got ~44% support — extract accurately."""
    text = (FIXTURES / "proxy_escalating_minority.txt").read_text()
    result = parse_proxy(text, date(2023, 6, 8))
    # Find proposal with highest for_pct (the supply chain one at ~44%)
    if result.shareholder_proposals:
        max_for = max(p.vote_for_pct for p in result.shareholder_proposals)
        assert max_for > 30.0  # substantial minority support


# ---------------------------------------------------------------------------
# parse_proxy — clean fixture
# ---------------------------------------------------------------------------


def test_parse_clean_say_on_pay():
    text = (FIXTURES / "proxy_clean.txt").read_text()
    result = parse_proxy(text, date(2023, 4, 20))
    assert result.say_on_pay_pct is not None
    assert result.say_on_pay_pct > 90.0


def test_parse_clean_no_shareholder_proposals():
    text = (FIXTURES / "proxy_clean.txt").read_text()
    result = parse_proxy(text, date(2023, 4, 20))
    assert result.shareholder_proposals == []


def test_parse_clean_low_ceo_ratio():
    text = (FIXTURES / "proxy_clean.txt").read_text()
    result = parse_proxy(text, date(2023, 4, 20))
    assert result.ceo_pay_ratio is not None
    assert result.ceo_pay_ratio < 50.0


# ---------------------------------------------------------------------------
# parse_proxy — edge cases
# ---------------------------------------------------------------------------


def test_parse_empty_text():
    result = parse_proxy("", date(2023, 1, 1))
    assert result.say_on_pay_pct is None
    assert result.shareholder_proposals == []
    assert result.executive_comp_total is None


def test_parse_non_standard_format():
    """Proxy with bare percentage format (no vote count columns)."""
    text = """
Annual Meeting Proxy

SAY-ON-PAY:
Advisory Vote on Executive Compensation.
The Board recommends a vote FOR this proposal.
VOTE RESULTS:
Votes For: 12,000,000 (82.4%)
Votes Against: 2,400,000 (16.5%)
Abstentions: 150,000 (1.0%)

SHAREHOLDER PROPOSALS:

Proposal 3: Shareholder Proposal on Diversity and Inclusion Reporting
Submitted by: Calvert Research and Management
RESOLVED, that shareholders request the Board publish an annual diversity, equity, and inclusion report.
The Board recommends a vote AGAINST this proposal.
VOTE RESULTS:
Votes For: 8,000,000 (55.0%)
Votes Against: 6,200,000 (42.6%)
Abstentions: 350,000 (2.4%)
"""
    result = parse_proxy(text, date(2023, 1, 1))
    assert result.say_on_pay_pct is not None
    assert abs(result.say_on_pay_pct - 82.4) < 0.1
    assert len(result.shareholder_proposals) == 1
    assert result.shareholder_proposals[0].topic == "diversity"
    assert result.shareholder_proposals[0].passed is True  # 55% > 50%


def test_parse_proposal_passed_flag():
    """Proposals above 50% are marked passed=True."""
    text = """
Proposal 2: Shareholder Proposal on Worker Welfare
Submitted by: AFL-CIO Reserve Fund
RESOLVED, that shareholders request a worker welfare report.
The Board recommends a vote AGAINST this proposal.
VOTE RESULTS:
Votes For: 30,000,000 (52.1%)
Votes Against: 26,000,000 (45.2%)
Abstentions: 1,500,000 (2.6%)
"""
    result = parse_proxy(text, date(2023, 1, 1))
    assert len(result.shareholder_proposals) == 1
    assert result.shareholder_proposals[0].passed is True


def test_parse_proposal_not_passed_flag():
    """Proposals below 50% are marked passed=False."""
    text = """
Proposal 2: Shareholder Proposal on Worker Welfare
Submitted by: AFL-CIO Reserve Fund
RESOLVED, that shareholders request a worker welfare report.
The Board recommends a vote AGAINST this proposal.
VOTE RESULTS:
Votes For: 20,000,000 (34.5%)
Votes Against: 35,000,000 (60.3%)
Abstentions: 3,000,000 (5.2%)
"""
    result = parse_proxy(text, date(2023, 1, 1))
    assert len(result.shareholder_proposals) == 1
    assert result.shareholder_proposals[0].passed is False


# ---------------------------------------------------------------------------
# Regression: say-on-pay block also contains a shareholder proposal
# ---------------------------------------------------------------------------


def test_parse_say_on_pay_block_with_embedded_proposal():
    """If say-on-pay and a shareholder proposal share a block, both should be parsed.

    Regression for bug where `continue` after say-on-pay detection caused
    any shareholder proposal content in the same block to be silently dropped.
    """
    text = """
Proposal 1: Advisory Vote on Executive Compensation and Proposal 2 Combined Section.
Advisory vote on executive compensation (say-on-pay).
The Board recommends a vote FOR this proposal.
VOTE RESULTS:
Votes For: 50,000,000 (75.0%)
Votes Against: 15,000,000 (22.5%)
Abstentions: 1,500,000 (2.5%)

Shareholder Proposal on Worker Welfare
Submitted by: AFL-CIO Reserve Fund
RESOLVED, that shareholders request a report on worker health, safety, and wages.
The Board recommends a vote AGAINST this proposal.
VOTE RESULTS:
Votes For: 22,000,000 (33.0%)
Votes Against: 43,000,000 (64.5%)
Abstentions: 1,667,000 (2.5%)
"""
    result = parse_proxy(text, date(2023, 1, 1))
    assert result.say_on_pay_pct is not None
    assert abs(result.say_on_pay_pct - 75.0) < 0.1
    assert len(result.shareholder_proposals) >= 1
    topics = {p.topic for p in result.shareholder_proposals}
    assert "worker_welfare" in topics


# ---------------------------------------------------------------------------
# Regression: pipe-delimited percentage tables
# ---------------------------------------------------------------------------


def test_parse_pipe_delimited_pct():
    """Vote results in pipe-delimited format (e.g. 'For | 58.3%') should parse correctly."""
    text = """
Proposal 1: Advisory Vote on Executive Compensation.
The Board recommends a vote FOR this proposal.
VOTE RESULTS:
For | 74.2%
Against | 24.1%
Abstentions | 1.7%
"""
    result = parse_proxy(text, date(2023, 1, 1))
    assert result.say_on_pay_pct is not None
    assert abs(result.say_on_pay_pct - 74.2) < 0.1


def test_parse_pipe_delimited_shareholder_proposal():
    """Pipe-delimited vote tables in shareholder proposal blocks are parsed correctly."""
    text = """
Proposal 3: Shareholder Proposal on Political Spending Disclosure.
Submitted by: NorthStar Asset Management
RESOLVED, that shareholders request disclosure of all political contributions and lobbying expenditures.
The Board recommends a vote AGAINST this proposal.
VOTE RESULTS:
For | 28.5%
Against | 69.9%
Abstentions | 1.6%
"""
    result = parse_proxy(text, date(2023, 1, 1))
    assert len(result.shareholder_proposals) == 1
    prop = result.shareholder_proposals[0]
    assert abs(prop.vote_for_pct - 28.5) < 0.1
    assert abs(prop.vote_against_pct - 69.9) < 0.1
    assert prop.passed is False
    assert prop.topic == "political_spending"


# ---------------------------------------------------------------------------
# flag_escalating_minority
# ---------------------------------------------------------------------------


def test_escalating_3_year_series():
    """Strictly increasing 3-year series → True."""
    assert flag_escalating_minority([28.5, 36.7, 44.1]) is True


def test_escalating_2_year_series():
    assert flag_escalating_minority([30.0, 42.0]) is True


def test_not_escalating_flat():
    assert flag_escalating_minority([35.0, 35.0, 35.0]) is False


def test_not_escalating_decreasing():
    assert flag_escalating_minority([44.1, 36.7, 28.5]) is False


def test_not_escalating_mixed():
    assert flag_escalating_minority([28.5, 44.1, 36.7]) is False


def test_escalating_single_entry():
    """Single data point — cannot determine trend."""
    assert flag_escalating_minority([40.0]) is False


def test_escalating_empty():
    assert flag_escalating_minority([]) is False


def test_escalating_near_threshold():
    """Just barely increasing counts as escalating."""
    assert flag_escalating_minority([40.0, 40.1]) is True


def test_escalating_realistic_4_year():
    assert flag_escalating_minority([22.0, 31.5, 38.9, 45.2]) is True
