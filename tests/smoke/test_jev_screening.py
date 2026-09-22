"""
Live quality check for Jev-backed screening (M9 topics, M10 merger factors).

The unit tests prove the seam with a stub client. This file is the only place
the model's actual judgment is measured — and it measures it *against the
keyword implementation it replaces*, on the same cases, in the same run.

That comparison is the point. An absolute accuracy number says nothing about
whether the swap was worth making; "Jev beat keywords by N cases" does. If Jev
does not beat the baseline here, this change should not ship.

Excluded from the default run (the ``live`` marker). Needs TYPESAFE_API_KEY
and bills a handful of requests::

    TYPESAFE_API_KEY="your-real-key" PYTHONPATH=. .venv/bin/python -m pytest \\
        tests/smoke/test_jev_screening.py -m live -v -s --no-cov
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from cam.analysis.merger_screener import (
    jev_factor_detector,
    keyword_detector,
)
from cam.analysis.proxy_parser import (
    jev_topic_classifier,
    keyword_topic_classifier,
)

GOLD_PATH = Path(__file__).parent.parent / "fixtures" / "analysis" / "screening_gold.json"

# Measured keyword baselines (2026-09-21). Jev must clear these, not merely
# approach them — a paid call that ties a free substring match is not worth
# making.
KEYWORD_TOPIC_BASELINE = 0.60  # 9/15
KEYWORD_FACTOR_BASELINE = 0.62  # 5/8


requires_typesafe_key = pytest.mark.skipif(
    not os.environ.get("TYPESAFE_API_KEY"),
    reason="TYPESAFE_API_KEY not set",
)


def _gold() -> dict:
    return json.loads(GOLD_PATH.read_text())


def _client():
    from typesafe_sdk import TypeSafeClient

    return TypeSafeClient(api_key=os.environ["TYPESAFE_API_KEY"])


@pytest.mark.live
@requires_typesafe_key
def test_jev_topics_beat_the_keyword_baseline(capsys):
    """Classify every gold proposal both ways and compare."""
    cases = _gold()["proposal_topics"]
    texts = [c["text"] for c in cases]
    expected = [c["expected"] for c in cases]

    keyword = keyword_topic_classifier(texts)
    with _client() as client:
        jev = jev_topic_classifier(client=client)(texts)

    kw_hits = sum(g == e for g, e in zip(keyword, expected))
    jev_hits = sum(g == e for g, e in zip(jev, expected))

    with capsys.disabled():
        print(
            f"\n  M9 proposal topics — keyword {kw_hits}/{len(cases)}, jev {jev_hits}/{len(cases)}"
        )
        print(f"  {'':4} {'want':19} {'keyword':19} {'jev':19} note")
        for case, kw, jv in zip(cases, keyword, jev):
            want = case["expected"]
            mark = "ok  " if jv == want else "MISS"
            print(f"  {mark} {want:19} {kw:19} {jv:19} {case['note'][:44]}")

    assert jev_hits / len(cases) > KEYWORD_TOPIC_BASELINE, (
        f"Jev scored {jev_hits}/{len(cases)} against a keyword baseline of "
        f"{KEYWORD_TOPIC_BASELINE:.0%}; the swap does not pay for itself"
    )


@pytest.mark.live
@requires_typesafe_key
def test_jev_factors_beat_the_keyword_baseline(capsys):
    """Detect factors on every gold deal both ways and compare."""
    cases = _gold()["merger_factors"]

    with _client() as client:
        detector = jev_factor_detector(client=client)
        rows, kw_hits, jev_hits = [], 0, 0
        for case in cases:
            want = set(case["expected_factors"])
            kw, _ = keyword_detector(case["target"], case["deal"])
            jv, confidence = detector(case["target"], case["deal"])

            kw_hits += kw == want
            jev_hits += jv == want
            rows.append(
                (
                    "ok  " if jv == want else "MISS",
                    ",".join(sorted(want)) or "(none)",
                    ",".join(sorted(jv)) or "(none)",
                    max(confidence.values()) if confidence else 0.0,
                    case["note"][:40],
                )
            )

    with capsys.disabled():
        print(
            f"\n  M10 merger factors — keyword {kw_hits}/{len(cases)}, jev {jev_hits}/{len(cases)}"
        )
        for mark, want, got, peak, note in rows:
            print(f"  {mark} want={want:34} jev={got:34} peak={peak:.2f}  {note}")

    assert jev_hits / len(cases) > KEYWORD_FACTOR_BASELINE, (
        f"Jev scored {jev_hits}/{len(cases)} against a keyword baseline of "
        f"{KEYWORD_FACTOR_BASELINE:.0%}; the swap does not pay for itself"
    )


@pytest.mark.live
@requires_typesafe_key
def test_negation_and_coincidence_are_the_cases_that_matter():
    """The two false positives substring matching cannot avoid.

    Separated from the aggregate because these are the specific defects that
    motivated the change: a deal that *denies* operating a marketplace, and a
    deal mentioning insurance only as a funding source. Both score weight
    under the keyword detector. If Jev does not fix these, it has not bought
    anything the keyword table could not do.
    """
    cases = {c["note"].split(";")[0]: c for c in _gold()["merger_factors"]}
    negation = next(c for k, c in cases.items() if k.startswith("negation"))
    coincidence = next(c for k, c in cases.items() if k.startswith("bare-word"))

    with _client() as client:
        detector = jev_factor_detector(client=client)
        for case in (negation, coincidence):
            factors, _ = detector(case["target"], case["deal"])
            assert factors == set(), f"{case['note']}: Jev still fired {sorted(factors)}"


@pytest.mark.live
@requires_typesafe_key
def test_a_filing_costs_one_request(capsys):
    """Confirm the batching claim for a realistic multi-proposal proxy."""
    from cam import jev
    from cam.analysis.proxy_parser import build_topic_questions

    texts = [c["text"] for c in _gold()["proposal_topics"]]
    with _client() as client:
        response = jev.ask(
            {"proposals": [{"text": t} for t in texts]},
            build_topic_questions(texts),
            client=client,
        )

    assert len(response.choices) == len(texts)
    with capsys.disabled():
        print(f"\n  {len(texts)} proposals, {len(texts)} questions, 1 request — {response.usage}")
