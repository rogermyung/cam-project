"""
Unit tests for M8 — Earnings Call NLP (cam/analysis/earnings_nlp.py).

All encoder calls are mocked — no live model downloads required.
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pytest

from cam.analysis.earnings_nlp import (
    EXTRACTION_PATTERNS,
    PatternHit,
    TranscriptScore,
    _get_sentences,
    _normalise,
    compute_divergence,
    score_transcript,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "edgar"

# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _hash_encoder(sentences: list[str]) -> np.ndarray:
    """Deterministic encoder: each unique sentence gets a unique unit vector."""
    dim = 384
    result = []
    for s in sentences:
        rng = np.random.RandomState(abs(hash(s)) % (2**31))
        vec = rng.randn(dim)
        result.append(vec / np.linalg.norm(vec))
    return np.array(result)


def _constant_encoder(sentences: list[str]) -> np.ndarray:
    """All sentences map to the same vector → cosine similarity 1.0 → divergence 0."""
    dim = 384
    vec = np.ones(dim) / np.sqrt(dim)
    return np.tile(vec, (len(sentences), 1))


def _zero_encoder(sentences: list[str]) -> np.ndarray:
    """All zero vectors — tests edge case handling."""
    return np.zeros((len(sentences), 384))


# ---------------------------------------------------------------------------
# _normalise
# ---------------------------------------------------------------------------


def test_normalise_collapses_whitespace():
    assert _normalise("  Hello   World  ") == "hello world"


def test_normalise_lowercases():
    assert _normalise("CAPTIVE Network") == "captive network"


# ---------------------------------------------------------------------------
# _get_sentences
# ---------------------------------------------------------------------------


def test_get_sentences_splits_on_period():
    text = "First sentence. Second sentence. Third sentence."
    sents = _get_sentences(text)
    assert len(sents) == 3


def test_get_sentences_empty():
    assert _get_sentences("") == []


def test_get_sentences_no_trailing_empty():
    sents = _get_sentences("Hello world.")
    assert all(s for s in sents)


# ---------------------------------------------------------------------------
# score_transcript — pattern detection
# ---------------------------------------------------------------------------


def test_score_transcript_captive_patterns_fire_on_cvs():
    """CVS fixture should trigger captive_strategy patterns."""
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    assert isinstance(result, TranscriptScore)
    assert result.pattern_hits["captive_strategy"]
    # All hit objects must be PatternHit instances
    for hit in result.pattern_hits["captive_strategy"]:
        assert isinstance(hit, PatternHit)
        assert hit.category == "captive_strategy"
        assert hit.score == 1.0


def test_score_transcript_labor_patterns_fire_on_cvs():
    """CVS fixture mentions headcount optimization and workforce rationalization."""
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    assert result.pattern_hits["labor_cost_extraction"]


def test_score_transcript_margin_patterns_fire_on_cvs():
    """CVS fixture mentions rebate retention, take rate, and capture rate."""
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    assert result.pattern_hits["margin_extraction"]


def test_score_transcript_neutral_has_lower_score():
    """Neutral transcripts should produce lower overall_score than CVS."""
    cvs = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    neutral_a = (FIXTURES / "earnings_neutral_a.txt").read_text()
    neutral_b = (FIXTURES / "earnings_neutral_b.txt").read_text()

    cvs_result = score_transcript(cvs)
    neutral_a_result = score_transcript(neutral_a)
    neutral_b_result = score_transcript(neutral_b)

    assert cvs_result.overall_score > neutral_a_result.overall_score
    assert cvs_result.overall_score > neutral_b_result.overall_score


def test_score_transcript_overall_score_in_unit_interval():
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    assert 0.0 <= result.overall_score <= 1.0


def test_score_transcript_all_categories_present():
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    assert set(result.pattern_hits.keys()) == set(EXTRACTION_PATTERNS.keys())


def test_score_transcript_pattern_hit_has_context():
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    for hits in result.pattern_hits.values():
        for hit in hits:
            assert hit.context  # non-empty context
            assert hit.pattern.lower() in hit.context.lower()


def test_score_transcript_empty_text():
    result = score_transcript("")
    assert result.overall_score == 0.0
    assert all(v == [] for v in result.pattern_hits.values())


def test_score_transcript_no_matches_text():
    result = score_transcript(
        "The weather today is sunny and warm with pleasant temperatures throughout the day."
    )
    assert result.overall_score == 0.0
    assert all(v == [] for v in result.pattern_hits.values())


def test_score_transcript_custom_patterns():
    custom = {"test_category": ["sunny", "warm"]}
    result = score_transcript("The weather is sunny and warm today.", patterns=custom)
    assert result.pattern_hits["test_category"]
    assert result.overall_score == 1.0


def test_score_transcript_deduplication():
    """Same (phrase, sentence) pair should only appear once."""
    # Construct a text where 'captive' appears twice in the same sentence
    text = "The captive model and captive network drive retention in our captive relationships."
    result = score_transcript(text, patterns={"captive_strategy": ["captive"]})
    # 'captive' appears 3 times but only 1 unique sentence — all map to same sentence
    # dedup is per (phrase, sentence) so we may get 3 hits if sentence is different
    # The key assertion: no duplicate (pattern, text) pairs
    hits = result.pattern_hits["captive_strategy"]
    seen = set()
    for h in hits:
        key = (h.pattern, h.text)
        assert key not in seen, f"Duplicate hit: {key}"
        seen.add(key)


def test_score_transcript_non_standard_formatting():
    """Transcripts with unusual formatting (no punctuation, ALL CAPS) handled gracefully."""
    text = (
        "MANAGEMENT We have a CAPTIVE NETWORK of preferred pharmacies "
        "HEADCOUNT OPTIMIZATION is key to our VARIABLE LABOR MODEL "
        "REBATE RETENTION drives our spread income and take rate economics"
    )
    result = score_transcript(text)
    # Should not raise; may or may not have hits depending on normalisation
    assert isinstance(result, TranscriptScore)
    assert 0.0 <= result.overall_score <= 1.0


def test_score_transcript_divergence_score_none_by_default():
    """score_transcript alone does not compute divergence."""
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    result = score_transcript(text)
    assert result.divergence_score is None


# ---------------------------------------------------------------------------
# compute_divergence
# ---------------------------------------------------------------------------


def test_divergence_identical_texts_near_zero():
    """Same text on both sides → near-zero divergence (constant encoder gives exact 0)."""
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    score = compute_divergence(text, text, encoder=_constant_encoder)
    assert score == pytest.approx(0.0, abs=1e-6)


def test_divergence_cvs_higher_than_neutral():
    """Extraction-language-heavy transcript diverges more from regulatory text.

    Uses a semantically-aware mock encoder: sentences containing extraction
    pattern keywords map to one pole; all other sentences map to the opposite
    pole. CVS transcript (many extraction phrases) → high divergence from 10-K
    (no extraction phrases). Neutral transcript → low divergence from 10-K.
    """
    _all_patterns = [p for phrases in EXTRACTION_PATTERNS.values() for p in phrases]

    def _extraction_aware_encoder(sentences: list[str]) -> np.ndarray:
        dim = 384
        vec_a = np.zeros(dim)
        vec_a[0] = 1.0  # "investor extraction language" pole
        vec_b = np.zeros(dim)
        vec_b[1] = 1.0  # "regulatory language" pole
        result = []
        for s in sentences:
            s_lower = s.lower()
            if any(p.lower() in s_lower for p in _all_patterns):
                result.append(vec_a)
            else:
                result.append(vec_b)
        return np.array(result)

    regulatory = (FIXTURES / "earnings_10k_cvs_excerpt.txt").read_text()
    cvs_transcript = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    neutral_transcript = (FIXTURES / "earnings_neutral_a.txt").read_text()

    cvs_div = compute_divergence(cvs_transcript, regulatory, encoder=_extraction_aware_encoder)
    neutral_div = compute_divergence(
        neutral_transcript, regulatory, encoder=_extraction_aware_encoder
    )

    assert cvs_div > neutral_div


def test_divergence_in_unit_interval():
    transcript = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    regulatory = (FIXTURES / "earnings_10k_cvs_excerpt.txt").read_text()
    score = compute_divergence(transcript, regulatory, encoder=_hash_encoder)
    assert 0.0 <= score <= 1.0


def test_divergence_empty_transcript():
    regulatory = (FIXTURES / "earnings_10k_cvs_excerpt.txt").read_text()
    score = compute_divergence("", regulatory, encoder=_hash_encoder)
    assert score == 0.0


def test_divergence_empty_regulatory():
    transcript = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    score = compute_divergence(transcript, "", encoder=_hash_encoder)
    assert score == 0.0


def test_divergence_zero_vectors_safe():
    """Zero-vector encoder should not raise ZeroDivisionError."""
    text = "Some text about operations and margins."
    score = compute_divergence(text, text, encoder=_zero_encoder)
    assert score == 0.0


def test_divergence_custom_topics():
    transcript = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    regulatory = (FIXTURES / "earnings_10k_cvs_excerpt.txt").read_text()
    score = compute_divergence(
        transcript,
        regulatory,
        encoder=_hash_encoder,
        topics=["pharmacy", "network", "rebate"],
    )
    assert 0.0 <= score <= 1.0


def test_divergence_symmetric_approximately():
    """Divergence should be symmetric: d(A,B) ≈ d(B,A)."""
    a = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    b = (FIXTURES / "earnings_10k_cvs_excerpt.txt").read_text()
    d_ab = compute_divergence(a, b, encoder=_hash_encoder)
    d_ba = compute_divergence(b, a, encoder=_hash_encoder)
    assert abs(d_ab - d_ba) < 1e-9


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def test_performance_score_transcript_under_5_seconds():
    """Pattern scoring of a full transcript should complete in < 5s."""
    text = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    start = time.time()
    score_transcript(text)
    assert time.time() - start < 5.0


def test_performance_compute_divergence_under_30_seconds():
    """Divergence computation with mock encoder should complete in < 30s."""
    transcript = (FIXTURES / "earnings_cvs_2023.txt").read_text()
    regulatory = (FIXTURES / "earnings_10k_cvs_excerpt.txt").read_text()
    start = time.time()
    compute_divergence(transcript, regulatory, encoder=_hash_encoder)
    assert time.time() - start < 30.0
