"""
Earnings Call NLP (M8).

Detects value-extraction language in earnings call transcripts and measures
semantic divergence between investor-facing and regulatory-facing (10-K)
language on the same topics.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Extraction patterns
# ---------------------------------------------------------------------------

EXTRACTION_PATTERNS: dict[str, list[str]] = {
    "captive_strategy": [
        "captive",
        "locked in",
        "sticky",
        "fully engaged member",
        "cross-sell",
        "self-referral",
        "in-network steering",
        "captive network",
        "preferred network",
    ],
    "labor_cost_extraction": [
        "labor efficiency",
        "headcount optimization",
        "workforce rationalization",
        "labor productivity",
        "right-sizing",
        "restructuring charges",
        "variable labor model",
        "contractor conversion",
    ],
    "margin_extraction": [
        "spread compression",
        "rebate retention",
        "spread income",
        "take rate",
        "monetization",
        "capture rate",
    ],
    "regulatory_arbitrage": [
        "regulatory environment",
        "regulatory flexibility",
        "light-touch regulation",
        "favorable regulatory",
        "offshore",
        "restructuring for regulatory",
    ],
}

# Minimum context window (chars) to include around each match
_CONTEXT_WINDOW = 150

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class PatternHit:
    """Single pattern match within a transcript."""

    pattern: str  # matched keyword/phrase
    category: str  # which EXTRACTION_PATTERNS key
    text: str  # sentence containing the match
    context: str  # surrounding context (±_CONTEXT_WINDOW chars)
    score: float  # 1.0 for exact phrase match


@dataclass
class TranscriptScore:
    """Result of scoring an earnings call transcript."""

    overall_score: float  # 0–1, fraction of pattern categories triggered
    pattern_hits: dict[str, list[PatternHit]] = field(default_factory=dict)
    divergence_score: float | None = None  # vs same company's 10-K language


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise(text: str) -> str:
    """Collapse whitespace and lower-case for matching."""
    return re.sub(r"\s+", " ", text).strip().lower()


def _get_sentences(text: str) -> list[str]:
    """Split on sentence boundaries; preserve originals for context."""
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]


def _sentence_spans(text: str) -> list[tuple[int, int, str]]:
    """Return (start, end, sentence_text) for each sentence in the original text.

    Iterates sentence-boundary splits while tracking cumulative character
    offsets so that span indices are valid for slicing ``text`` directly.
    """
    spans: list[tuple[int, int, str]] = []
    pos = 0
    for raw in re.split(r"(?<=[.!?])\s+", text):
        stripped = raw.strip()
        if not stripped:
            pos += len(raw) + 1  # +1 for the split whitespace consumed
            continue
        start = text.find(stripped, pos)
        if start == -1:
            start = pos
        end = start + len(stripped)
        spans.append((start, end, stripped))
        pos = end
    return spans


def _phrase_pattern(phrase: str) -> re.Pattern[str]:
    """Build a case-insensitive regex for ``phrase``, allowing any whitespace between words."""
    parts = [re.escape(w) for w in phrase.split()]
    return re.compile(r"\s+".join(parts), re.IGNORECASE)


def _extract_context(text: str, match_start: int, match_end: int) -> str:
    """Return ±_CONTEXT_WINDOW chars around the match."""
    start = max(0, match_start - _CONTEXT_WINDOW)
    end = min(len(text), match_end + _CONTEXT_WINDOW)
    return text[start:end].strip()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def score_transcript(
    transcript_text: str,
    patterns: dict[str, list[str]] | None = None,
) -> TranscriptScore:
    """Score an earnings call transcript for value-extraction language.

    Parameters
    ----------
    transcript_text:
        Raw transcript text (plain text).
    patterns:
        Extraction pattern dict. Defaults to :data:`EXTRACTION_PATTERNS`.

    Returns
    -------
    TranscriptScore with overall_score, pattern_hits, and divergence_score=None.
    """
    if patterns is None:
        patterns = EXTRACTION_PATTERNS

    # Pre-compute sentence spans with correct offsets into the original text
    sent_spans = _sentence_spans(transcript_text)

    hits: dict[str, list[PatternHit]] = {cat: [] for cat in patterns}

    for cat, phrases in patterns.items():
        for phrase in phrases:
            phrase_re = _phrase_pattern(phrase)
            # Search directly in original text — offsets are valid for context slicing
            for m in phrase_re.finditer(transcript_text):
                context = _extract_context(transcript_text, m.start(), m.end())
                # Find the sentence whose span contains this specific match
                sentence = ""
                for s_start, s_end, s_text in sent_spans:
                    if s_start <= m.start() < s_end:
                        sentence = s_text
                        break
                hits[cat].append(
                    PatternHit(
                        pattern=phrase,
                        category=cat,
                        text=sentence,
                        context=context,
                        score=1.0,
                    )
                )

    # Deduplicate: one hit per (category, phrase) to avoid double-counting
    # if the phrase appears multiple times — keep all occurrences but deduplicate
    # exact same (phrase, sentence) pairs
    deduped: dict[str, list[PatternHit]] = {}
    for cat, cat_hits in hits.items():
        seen: set[tuple[str, str]] = set()
        deduped[cat] = []
        for h in cat_hits:
            key = (h.pattern, h.text)
            if key not in seen:
                seen.add(key)
                deduped[cat].append(h)

    # overall_score = fraction of pattern categories that have ≥1 hit
    triggered = sum(1 for v in deduped.values() if v)
    overall_score = triggered / len(patterns) if patterns else 0.0

    return TranscriptScore(
        overall_score=overall_score,
        pattern_hits=deduped,
    )


def compute_divergence(
    transcript_text: str,
    regulatory_text: str,
    *,
    encoder: Callable | None = None,
    topics: list[str] | None = None,
) -> float:
    """Measure semantic divergence between investor- and regulatory-facing language.

    Uses cosine distance between topic-averaged sentence embeddings.
    Higher score = more divergence = higher concern.

    Parameters
    ----------
    transcript_text:
        Earnings call transcript (investor-facing language).
    regulatory_text:
        10-K or proxy filing text (regulatory-facing language).
    encoder:
        Injectable sentence encoder callable: ``encoder(list[str]) -> ndarray``.
        Defaults to ``sentence-transformers/all-MiniLM-L6-v2``.
    topics:
        Seed phrases used to select topically relevant sentences from each text.
        Defaults to high-signal financial terms.

    Returns
    -------
    float in [0, 1]: 0 = identical language, 1 = maximally divergent.
    """
    import numpy as np

    if topics is None:
        topics = [
            "cost",
            "labor",
            "regulatory",
            "pricing",
            "network",
            "margin",
            "competition",
        ]

    enc = encoder if encoder is not None else _default_encoder()

    transcript_sents = _get_sentences(transcript_text)
    regulatory_sents = _get_sentences(regulatory_text)

    if not transcript_sents or not regulatory_sents:
        return 0.0

    # Filter to sentences containing at least one topic keyword
    def _topic_filter(sentences: list[str]) -> list[str]:
        filtered = [s for s in sentences if any(t.lower() in s.lower() for t in topics)]
        return filtered if filtered else sentences  # fall back to all if nothing matches

    t_sents = _topic_filter(transcript_sents)
    r_sents = _topic_filter(regulatory_sents)

    t_embs = np.array(enc(t_sents))
    r_embs = np.array(enc(r_sents))

    # Mean-pool to get single representation per document
    t_mean = t_embs.mean(axis=0)
    r_mean = r_embs.mean(axis=0)

    # Cosine similarity → cosine distance (divergence)
    norm_t = np.linalg.norm(t_mean)
    norm_r = np.linalg.norm(r_mean)
    if norm_t < 1e-9 or norm_r < 1e-9:
        return 0.0

    cosine_sim = float(np.dot(t_mean, r_mean) / (norm_t * norm_r))
    # Clamp to [0, 1] range: sim in [-1,1] → distance in [0, 1]
    cosine_sim = max(-1.0, min(1.0, cosine_sim))
    divergence = (1.0 - cosine_sim) / 2.0
    return float(divergence)


# ---------------------------------------------------------------------------
# Lazy model loading
# ---------------------------------------------------------------------------

_encoder_cache: object | None = None


def _default_encoder() -> object:
    global _encoder_cache
    if _encoder_cache is None:
        from sentence_transformers import SentenceTransformer

        from cam.config import Settings as _Settings

        model_name = _Settings.model_fields["risk_encoder_model"].default
        _encoder_cache = SentenceTransformer(model_name)
    return _encoder_cache
