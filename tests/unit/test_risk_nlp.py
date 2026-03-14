"""
Unit tests for M7 — 10-K Risk Language NLP (cam/analysis/risk_nlp.py).

All model calls are mocked — no live model downloads required.
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np

from cam.analysis.risk_nlp import (
    RISK_TOPICS,
    RiskExpansionResult,
    _split_sentences,
    _strip_html,
    classify_risk_topics,
    compute_risk_expansion,
    extract_risk_section,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "edgar"

# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _hash_encoder(sentences: list[str]) -> np.ndarray:
    """Deterministic encoder: each unique sentence gets a unique unit vector.

    Sentences with the same text map to the same vector (cosine sim = 1.0).
    Sentences with different text are near-orthogonal in high dimensions.
    """
    dim = 384
    result = []
    for s in sentences:
        rng = np.random.RandomState(abs(hash(s)) % (2**31))
        vec = rng.randn(dim)
        result.append(vec / np.linalg.norm(vec))
    return np.array(result)


def _constant_encoder(sentences: list[str]) -> np.ndarray:
    """All sentences map to the same vector → cosine similarity always 1.0."""
    dim = 384
    vec = np.ones(dim) / np.sqrt(dim)
    return np.tile(vec, (len(sentences), 1))


def _mock_classifier(topic_scores: dict[str, float] | None = None) -> object:
    """Return a mock classifier that always returns the given topic scores."""
    fixed = topic_scores or {t: 0.5 for t in RISK_TOPICS}

    def classify(text: str, topics: list[str], multi_label: bool = True) -> dict:
        return {
            "labels": topics,
            "scores": [fixed.get(t, 0.0) for t in topics],
        }

    return classify


# ---------------------------------------------------------------------------
# _strip_html
# ---------------------------------------------------------------------------


def test_strip_html_removes_tags():
    html = "<p>Hello <b>world</b>!</p>"
    assert "Hello" in _strip_html(html)
    assert "<" not in _strip_html(html)


def test_strip_html_unescapes_entities():
    html = "<p>AT&amp;T &lt;Company&gt;</p>"
    result = _strip_html(html)
    assert "AT&T" in result
    assert "&amp;" not in result


def test_strip_html_plain_text_passthrough():
    text = "This is plain text with no tags."
    # _strip_html should handle it without error
    result = _strip_html(text)
    assert "plain text" in result


# ---------------------------------------------------------------------------
# _split_sentences
# ---------------------------------------------------------------------------


def test_split_sentences_basic():
    text = "The company faces significant competition in all of its markets. It may lose market share to competitors with greater resources. Short."
    sentences = _split_sentences(text)
    # "Short." has only 1 word, should be filtered
    assert len(sentences) == 2
    assert all(len(s.split()) >= 8 for s in sentences)


def test_split_sentences_empty():
    assert _split_sentences("") == []


def test_split_sentences_min_word_filter():
    text = (
        "Too short. " + "This sentence is long enough to pass the minimum word count filter here."
    )
    sentences = _split_sentences(text)
    assert len(sentences) == 1


# ---------------------------------------------------------------------------
# extract_risk_section — plain text
# ---------------------------------------------------------------------------


def test_extract_risk_section_plain_item_1a():
    text = """
ITEM 1. BUSINESS
Some business description here.

ITEM 1A. RISK FACTORS
The company faces significant competitive risks in its primary markets.
Changes in regulation could adversely affect operations and profitability.

ITEM 1B. UNRESOLVED STAFF COMMENTS
None.
"""
    section = extract_risk_section(text)
    assert "competitive risks" in section
    assert "ITEM 1B" not in section
    assert "ITEM 1." not in section


def test_extract_risk_section_case_insensitive():
    text = "Item 1A. Risk Factors\nThe company is exposed to various risks.\nItem 2. Properties\nSome property details."
    section = extract_risk_section(text)
    assert "exposed to various risks" in section
    assert "Properties" not in section


def test_extract_risk_section_heuristic_fallback():
    """When 'Item 1A' header is absent, fall back to 'Risk Factors'."""
    text = "Annual Report\n\nRisk Factors\nThe company faces many risks in its operations and markets.\n\nFinancial Statements\nSome financials."
    section = extract_risk_section(text)
    assert "faces many risks" in section


def test_extract_risk_section_no_header_returns_full_text():
    """When no risk header is found, return the full text."""
    text = "This document has no risk section at all and mentions nothing relevant."
    section = extract_risk_section(text)
    assert len(section) > 0


def test_extract_risk_section_html_format():
    html_path = FIXTURES / "risk_section_html.html"
    html = html_path.read_text()
    section = extract_risk_section(html)
    assert "environmental regulations" in section.lower() or "competition" in section.lower()
    assert "<" not in section  # HTML tags stripped


def test_extract_risk_section_from_expansion_fixture():
    """Verify section extraction works on the expansion fixture."""
    text = (FIXTURES / "risk_section_expansion_current.txt").read_text()
    # The fixture IS the risk section already, but wrap it in a full filing format
    wrapped = f"ITEM 1. BUSINESS\nSome text.\n\nITEM 1A. RISK FACTORS\n{text}\n\nITEM 1B. UNRESOLVED STAFF COMMENTS\nNone."
    section = extract_risk_section(wrapped)
    assert "Department of Justice" in section


# ---------------------------------------------------------------------------
# classify_risk_topics
# ---------------------------------------------------------------------------


def test_classify_risk_topics_returns_all_topics():
    result = classify_risk_topics(
        "Some text about labor disputes and wage issues.",
        RISK_TOPICS,
        classifier=_mock_classifier(),
    )
    assert set(result.keys()) == set(RISK_TOPICS)
    assert all(0.0 <= v <= 1.0 for v in result.values())


def test_classify_risk_topics_empty_text():
    result = classify_risk_topics("", RISK_TOPICS, classifier=_mock_classifier())
    assert all(v == 0.0 for v in result.values())


def test_classify_risk_topics_empty_topics():
    result = classify_risk_topics(
        "Some text here about important business risks.", [], classifier=_mock_classifier()
    )
    assert result == {}


def test_classify_risk_topics_score_mapping():
    """Ensure topic scores are correctly mapped from classifier output."""
    fixed = {t: float(i) / len(RISK_TOPICS) for i, t in enumerate(RISK_TOPICS)}
    result = classify_risk_topics(
        "Test sentence.",
        RISK_TOPICS,
        classifier=_mock_classifier(fixed),
    )
    for topic in RISK_TOPICS:
        assert abs(result[topic] - fixed[topic]) < 1e-9


# ---------------------------------------------------------------------------
# compute_risk_expansion — expansion pair (significant new content)
# ---------------------------------------------------------------------------


def test_expansion_detected_with_new_sentences():
    """Texts with mostly new sentences should produce high expansion score."""
    prior = "The company faces competition in all markets. Prices may be reduced to maintain market share."
    current = (
        "The company faces competition in all markets. Prices may be reduced to maintain market share. "
        "The company has received a formal investigation notice from the Department of Justice antitrust division. "
        "Wage theft lawsuits have been filed by current and former employees in multiple jurisdictions. "
        "Environmental enforcement actions are pending at three manufacturing facilities in Ohio and Texas. "
        "Regulators are probing market concentration practices and pricing coordination with competitors."
    )
    result = compute_risk_expansion(
        current,
        prior,
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    assert isinstance(result, RiskExpansionResult)
    assert result.expansion_score > 0.3
    assert len(result.new_sentences) > 0
    assert len(result.evidence) == len(result.new_sentences)


def test_expansion_fixture_pair():
    """End-to-end: expansion fixture pair should produce higher score than stable pair."""
    exp_prior = (FIXTURES / "risk_section_expansion_prior.txt").read_text()
    exp_current = (FIXTURES / "risk_section_expansion_current.txt").read_text()
    stable_prior = (FIXTURES / "risk_section_stable_prior.txt").read_text()
    stable_current = (FIXTURES / "risk_section_stable_current.txt").read_text()

    exp_result = compute_risk_expansion(
        exp_current, exp_prior, encoder=_hash_encoder, classifier=_mock_classifier()
    )
    stable_result = compute_risk_expansion(
        stable_current, stable_prior, encoder=_hash_encoder, classifier=_mock_classifier()
    )

    assert exp_result.expansion_score > stable_result.expansion_score


# ---------------------------------------------------------------------------
# compute_risk_expansion — stable pair (minimal change)
# ---------------------------------------------------------------------------


def test_stable_pair_low_expansion():
    """Identical texts should produce zero expansion."""
    text = (
        "The company faces significant competition from established market participants worldwide. "
        "The company relies on key personnel for its strategic direction and operations. "
        "The company is subject to extensive regulatory requirements across all jurisdictions."
    )
    result = compute_risk_expansion(
        text,
        text,  # same text for prior and current
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    assert result.expansion_score == 0.0
    assert result.new_sentences == []


def test_constant_encoder_all_similar():
    """When encoder returns identical vectors, all sentences match and expansion = 0."""
    prior = "The company faces competitive risk in its primary business segments and markets."
    current = "The company faces competitive risk in its primary business segments and markets. Additional risk from new entrants."
    result = compute_risk_expansion(
        current,
        prior,
        encoder=_constant_encoder,
        classifier=_mock_classifier(),
    )
    assert result.expansion_score == 0.0


# ---------------------------------------------------------------------------
# compute_risk_expansion — edge cases
# ---------------------------------------------------------------------------


def test_empty_current_text():
    result = compute_risk_expansion(
        "",
        "Some prior year risk disclosure text about competition and regulation.",
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    assert result.expansion_score == 0.0
    assert result.new_sentences == []
    assert set(result.topic_scores.keys()) == set(RISK_TOPICS)


def test_empty_prior_text():
    """With no prior text, all current sentences should be 'new'."""
    current = (
        "The company faces regulatory risk across all of its operating jurisdictions. "
        "Labor disputes have increased significantly over the past fiscal year period."
    )
    result = compute_risk_expansion(
        current,
        "",
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    assert result.expansion_score == 1.0
    assert len(result.new_sentences) == len(_split_sentences(current))


def test_custom_similarity_threshold():
    """Lower threshold → fewer new sentences detected."""
    prior = "The company faces significant competitive risk in all its primary markets."
    current = (
        "The company faces significant competitive risk in all its primary markets. "
        "An entirely different sentence about regulatory probe from government agencies."
    )
    result_strict = compute_risk_expansion(
        current,
        prior,
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
        similarity_threshold=0.99,
    )
    result_loose = compute_risk_expansion(
        current,
        prior,
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
        similarity_threshold=0.01,
    )
    # strict threshold: more sentences detected as new
    # loose threshold: almost nothing is new
    assert result_strict.expansion_score >= result_loose.expansion_score


def test_topic_scores_present_for_all_topics():
    current = "The company received a subpoena from the Department of Justice antitrust division."
    result = compute_risk_expansion(
        current,
        "",
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    assert set(result.topic_scores.keys()) == set(RISK_TOPICS)
    assert all(0.0 <= v <= 1.0 for v in result.topic_scores.values())


def test_expansion_score_clamped_to_unit_interval():
    current = " ".join(
        [
            f"Sentence number {i} about regulatory investigation and enforcement actions."
            for i in range(20)
        ]
    )
    result = compute_risk_expansion(
        current,
        "",
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    assert 0.0 <= result.expansion_score <= 1.0


def test_evidence_structure():
    """Each evidence item must have 'text' and 'topics' keys."""
    current = "The Department of Justice has opened a formal antitrust investigation into our pricing practices."
    result = compute_risk_expansion(
        current,
        "",
        encoder=_hash_encoder,
        classifier=_mock_classifier(),
    )
    for item in result.evidence:
        assert "text" in item
        assert "topics" in item
        assert isinstance(item["topics"], dict)


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_performance_under_30_seconds():
    """Processing a filing pair with ~50 sentences each should finish in < 30s."""
    # With mock encoder/classifier, this is fast regardless of sentence count
    base = "The company is subject to significant regulatory and legal risks in all jurisdictions where it operates."
    current = " ".join([f"{base} ({i})" for i in range(50)])
    prior = " ".join([f"{base} ({i})" for i in range(40)])

    start = time.time()
    compute_risk_expansion(current, prior, encoder=_hash_encoder, classifier=_mock_classifier())
    elapsed = time.time() - start

    assert elapsed < 30.0, f"compute_risk_expansion took {elapsed:.2f}s (limit: 30s)"
