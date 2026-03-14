"""
10-K Risk Language NLP (M7).

Functions for extracting and analysing year-over-year changes in 10-K risk
factor language to detect emerging labour, environmental, and consumer-harm
signals.

All model calls are injectable for testing: pass ``encoder`` / ``classifier``
keyword arguments to avoid loading heavy HuggingFace models in CI.
"""

from __future__ import annotations

import html as html_lib
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from html.parser import HTMLParser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RISK_TOPICS: list[str] = [
    "labor_relations",  # union activity, worker complaints, wage disputes
    "regulatory_investigation",  # government probe, subpoena, inquiry
    "supply_chain_labor",  # supplier labour practices, forced labour
    "environmental_liability",  # contamination, cleanup, EPA action
    "consumer_harm",  # product liability, consumer complaints, fraud
    "antitrust_competition",  # market concentration, DOJ/FTC investigation
]

try:
    from cam.config import Settings as _Settings

    _cfg = _Settings.model_fields
    SIMILARITY_THRESHOLD: float = _cfg["risk_similarity_threshold"].default  # type: ignore[assignment]
    MIN_SENTENCE_WORDS: int = _cfg["risk_min_sentence_words"].default  # type: ignore[assignment]
    _ENCODER_MODEL: str = _cfg["risk_encoder_model"].default  # type: ignore[assignment]
    _CLASSIFIER_MODEL: str = _cfg["risk_classifier_model"].default  # type: ignore[assignment]
except Exception:  # pragma: no cover
    SIMILARITY_THRESHOLD = 0.75
    MIN_SENTENCE_WORDS = 8
    _ENCODER_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
    _CLASSIFIER_MODEL = "facebook/bart-large-mnli"

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class RiskExpansionResult:
    """Result of year-over-year risk section comparison."""

    expansion_score: float  # fraction of current sentences that are new, 0–1
    new_sentences: list[str] = field(default_factory=list)
    topic_scores: dict[str, float] = field(default_factory=dict)  # per-topic mean score
    evidence: list[dict] = field(default_factory=list)  # {"text": …, "topics": {…}}


# ---------------------------------------------------------------------------
# HTML handling
# ---------------------------------------------------------------------------


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def _strip_html(text: str) -> str:
    """Remove HTML tags and unescape entities."""
    stripper = _HTMLStripper()
    stripper.feed(text)
    return html_lib.unescape(stripper.get_text())


# ---------------------------------------------------------------------------
# Section extraction
# ---------------------------------------------------------------------------

_ITEM_1A_START = re.compile(
    r"ITEM\s+1A\.?\s*(?:RISK\s+FACTORS?|Risk\s+Factors?)",
    re.IGNORECASE,
)
_NEXT_ITEM_START = re.compile(
    r"ITEM\s+\d+[A-Z]?\.?\b|PART\s+II\b|FINANCIAL\s+STATEMENTS\b",
    re.IGNORECASE,
)


def extract_risk_section(filing_text: str) -> str:
    """Extract Item 1A (Risk Factors) from a 10-K filing.

    Handles both HTML and plain-text EDGAR formats. When the standard
    ``Item 1A. Risk Factors`` header is absent, falls back to searching for
    any ``Risk Factors`` heading before returning the full text.

    Parameters
    ----------
    filing_text:
        Raw 10-K filing content (HTML or plain text).
    """
    # Normalise HTML → plain text (use tag regex, not bare < > check, to avoid
    # false positives on plain-text filings with comparison operators)
    _HTML_TAG_RE = re.compile(r"</?[a-zA-Z][^>]*>|<!DOCTYPE", re.IGNORECASE)
    text = _strip_html(filing_text) if _HTML_TAG_RE.search(filing_text) else filing_text

    # Collapse excessive whitespace for cleaner regex matching
    text = re.sub(r"[ \t]+", " ", text)

    # Primary: look for "Item 1A. Risk Factors"
    m_start = _ITEM_1A_START.search(text)

    if not m_start:
        # Heuristic fallback: bare "Risk Factors" heading
        m_start = re.search(r"\bRisk\s+Factors\b", text, re.IGNORECASE)
        if not m_start:
            logger.warning("Item 1A / Risk Factors not found; returning full text")
            return text.strip()

    start = m_start.end()

    m_end = _NEXT_ITEM_START.search(text, start)
    section = text[start : m_end.start()] if m_end else text[start:]

    return section.strip()


# ---------------------------------------------------------------------------
# Sentence splitting
# ---------------------------------------------------------------------------


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences, filtering stubs shorter than MIN_SENTENCE_WORDS."""
    raw = re.split(r"(?<=[.!?])\s+", text)
    return [s.strip() for s in raw if len(s.split()) >= MIN_SENTENCE_WORDS]


# ---------------------------------------------------------------------------
# Lazy model loading
# ---------------------------------------------------------------------------

_encoder_cache: object | None = None
_classifier_cache: object | None = None


def _default_encoder() -> object:
    global _encoder_cache
    if _encoder_cache is None:
        from sentence_transformers import SentenceTransformer

        _encoder_cache = SentenceTransformer(_ENCODER_MODEL)
    return _encoder_cache


def _default_classifier() -> object:
    global _classifier_cache
    if _classifier_cache is None:
        from transformers import pipeline

        _classifier_cache = pipeline(
            "zero-shot-classification",
            model=_CLASSIFIER_MODEL,
        )
    return _classifier_cache


# ---------------------------------------------------------------------------
# Topic classification
# ---------------------------------------------------------------------------


def classify_risk_topics(
    text: str,
    topics: list[str],
    *,
    classifier: Callable | None = None,
) -> dict[str, float]:
    """Classify text against risk topics using zero-shot classification.

    Parameters
    ----------
    text:
        Text snippet to classify.
    topics:
        List of topic label strings.
    classifier:
        Optional injectable classifier. Must be callable as
        ``clf(text, topics, multi_label=True)`` and return a dict with
        ``"labels"`` and ``"scores"`` lists.  Defaults to
        ``facebook/bart-large-mnli`` via ``transformers.pipeline``.

    Returns
    -------
    dict mapping each topic to a confidence score in [0, 1].
    """
    if not text.strip() or not topics:
        return {t: 0.0 for t in topics}

    clf = classifier if classifier is not None else _default_classifier()
    result = clf(text, topics, multi_label=True)
    return dict(zip(result["labels"], result["scores"]))


# ---------------------------------------------------------------------------
# Risk expansion
# ---------------------------------------------------------------------------


def compute_risk_expansion(
    current_text: str,
    prior_text: str,
    topics: list[str] | None = None,
    *,
    encoder: Callable | None = None,
    classifier: Callable | None = None,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
) -> RiskExpansionResult:
    """Detect year-over-year expansion in 10-K risk factor language.

    Algorithm
    ---------
    1. Split both texts into sentences (≥ MIN_SENTENCE_WORDS words).
    2. Encode all sentences with a sentence transformer.
    3. For each current-year sentence, compute its maximum cosine similarity
       to any prior-year sentence.
    4. Sentences with max similarity < ``similarity_threshold`` are "new".
    5. Run zero-shot topic classification on each new sentence.
    6. Return a :class:`RiskExpansionResult` with aggregate scores.

    Parameters
    ----------
    current_text:
        Risk section from the current-year 10-K.
    prior_text:
        Risk section from the prior-year 10-K.
    topics:
        Topic labels for zero-shot classification (default: :data:`RISK_TOPICS`).
    encoder:
        Injectable sentence encoder. Callable accepting ``list[str]`` and
        returning a 2-D array of shape ``(n, dim)``.  Defaults to
        ``sentence-transformers/all-MiniLM-L6-v2``.
    classifier:
        Injectable zero-shot classifier (see :func:`classify_risk_topics`).
    similarity_threshold:
        Cosine similarity below which a sentence counts as new (default 0.75).
    """
    import numpy as np

    if topics is None:
        topics = RISK_TOPICS

    current_sentences = _split_sentences(current_text)
    prior_sentences = _split_sentences(prior_text)

    if not current_sentences:
        return RiskExpansionResult(
            expansion_score=0.0,
            topic_scores={t: 0.0 for t in topics},
        )

    enc = encoder if encoder is not None else _default_encoder()

    # Encode current sentences; encode prior only if non-empty
    current_embeddings = np.array(enc(current_sentences))

    if prior_sentences:
        prior_embeddings = np.array(enc(prior_sentences))
        # Normalise rows for cosine similarity via dot product
        cur_normed = current_embeddings / np.maximum(
            np.linalg.norm(current_embeddings, axis=1, keepdims=True), 1e-9
        )
        pri_normed = prior_embeddings / np.maximum(
            np.linalg.norm(prior_embeddings, axis=1, keepdims=True), 1e-9
        )
        # (N_cur × N_pri) similarity matrix → best match per current sentence
        max_similarities: np.ndarray = (cur_normed @ pri_normed.T).max(axis=1)
    else:
        max_similarities = np.zeros(len(current_sentences))

    new_sentences = [
        s
        for s, sim in zip(current_sentences, max_similarities)
        if float(sim) < similarity_threshold
    ]

    expansion_score = len(new_sentences) / len(current_sentences)

    # Classify new sentences by topic
    topic_hit_scores: dict[str, list[float]] = {t: [] for t in topics}
    evidence: list[dict] = []

    for sentence in new_sentences:
        scores = classify_risk_topics(sentence, topics, classifier=classifier)
        evidence.append({"text": sentence, "topics": scores})
        for t in topics:
            topic_hit_scores[t].append(float(scores.get(t, 0.0)))

    topic_scores = {
        t: (sum(hits) / len(hits) if hits else 0.0) for t, hits in topic_hit_scores.items()
    }

    return RiskExpansionResult(
        expansion_score=expansion_score,
        new_sentences=new_sentences,
        topic_scores=topic_scores,
        evidence=evidence,
    )
