"""
Live quality check for Jev entity alignment.

Typed output guarantees the interface, not the truth.  The unit tests in
tests/unit/test_jev_align.py prove the wiring and the policy with a stub
client; this file is the only place the model's actual judgment is measured,
against the hand-labelled gold set in tests/fixtures/entity/alignment_gold.json.

Excluded from the default run (the ``live`` marker).  Needs TYPESAFE_API_KEY
and bills a handful of requests::

    PYTHONPATH=. .venv/bin/python -m pytest tests/smoke/test_jev_align.py -m live -v -s

Treat the floors below as this deployment's measured baseline, not as a
property of the model.  If a case regresses, read the printed per-case table
before touching a threshold: the fix is usually a clearer level description or
a missing candidate, not a looser bar.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from cam.entity.jev_align import (
    MERGE,
    REJECT,
    AlignmentCandidate,
    adjudicate,
    build_questions,
    build_state,
)

GOLD_PATH = Path(__file__).parent.parent / "fixtures" / "entity" / "alignment_gold.json"

# Measured baselines. Both are floors, not targets.
MIN_VERDICT_ACCURACY = 0.80
# A wrong merge attributes one company's violations to another, so it is the
# error this system can least afford. REVIEW instead of MERGE only costs a
# reviewer's time; MERGE onto the wrong entity corrupts the output.
MAX_WRONG_MERGES = 0


requires_typesafe_key = pytest.mark.skipif(
    not os.environ.get("TYPESAFE_API_KEY"),
    reason="TYPESAFE_API_KEY not set",
)


def _load_cases() -> list[dict]:
    return json.loads(GOLD_PATH.read_text())["cases"]


def _candidates(names: list[str]) -> list[AlignmentCandidate]:
    return [
        AlignmentCandidate(entity_id=uuid.uuid4(), canonical_name=name, prefilter_score=0.0)
        for name in names
    ]


@pytest.mark.live
@requires_typesafe_key
def test_alignment_accuracy_on_gold_set(capsys):
    """Measure verdict accuracy and count wrong merges across the gold set."""
    from typesafe_sdk import TypeSafeClient

    cases = _load_cases()
    rows: list[tuple] = []
    correct = 0
    wrong_merges = 0

    with TypeSafeClient(api_key=os.environ["TYPESAFE_API_KEY"]) as client:
        for case in cases:
            candidates = _candidates(case["candidates"])
            verdict = adjudicate(
                case["raw_name"],
                case["source"],
                candidates,
                client=client,
            )

            expected = case["expected"]
            got = verdict.verdict
            hit = got == expected

            # A merge onto an entity other than the labelled one is a wrong
            # merge even when the verdict code happens to match.
            if got == MERGE and expected == MERGE:
                hit = verdict.entity_id == candidates[case["match_index"]].entity_id
                if not hit:
                    wrong_merges += 1
            elif got == MERGE and expected != MERGE:
                wrong_merges += 1

            correct += hit
            rows.append(
                (
                    "ok " if hit else "MISS",
                    case["raw_name"][:34],
                    expected,
                    got,
                    f"{verdict.level:.2f}",
                    f"{verdict.confidence:.2f}",
                    (verdict.canonical_name or "-")[:30],
                )
            )

    accuracy = correct / len(cases)

    with capsys.disabled():
        print(f"\n  Jev entity alignment — {correct}/{len(cases)} = {accuracy:.1%}")
        print(f"  {'':4} {'raw_name':34} {'want':7} {'got':7} {'lvl':5} {'conf':5} selected")
        for row in rows:
            print("  {:4} {:34} {:7} {:7} {:5} {:5} {}".format(*row))

    assert wrong_merges <= MAX_WRONG_MERGES, (
        f"{wrong_merges} wrong merge(s) — a merge onto the wrong entity "
        f"attributes one company's violations to another"
    )
    assert accuracy >= MIN_VERDICT_ACCURACY, (
        f"verdict accuracy {accuracy:.1%} below the {MIN_VERDICT_ACCURACY:.0%} floor"
    )


@pytest.mark.live
@requires_typesafe_key
def test_placeholder_rows_are_screened_out():
    """The global raw_is_company Noul must reject non-company rows."""
    from typesafe_sdk import TypeSafeClient

    candidates = _candidates(["Tyson Foods, Inc.", "The Kroger Co.", "Amazon.com, Inc."])
    with TypeSafeClient(api_key=os.environ["TYPESAFE_API_KEY"]) as client:
        for raw in ("Unknown", "N/A", "Confidential", "Various"):
            verdict = adjudicate(raw, "warn", candidates, client=client)
            assert verdict.verdict == REJECT, f"{raw!r} -> {verdict.verdict}"


@pytest.mark.live
@requires_typesafe_key
def test_shortlist_costs_one_request(capsys):
    """Confirm the fan-out claim: N candidates, one round trip, one document."""
    from typesafe_sdk import TypeSafeClient

    candidates = _candidates(
        [
            "Albertsons Companies, Inc.",
            "The Kroger Co.",
            "Sprouts Farmers Market, Inc.",
            "Publix Super Markets, Inc.",
            "Weis Markets, Inc.",
        ]
    )
    with TypeSafeClient(api_key=os.environ["TYPESAFE_API_KEY"]) as client:
        response = client.system_one(
            state=build_state("SAFEWAY STORES 4680", "warn", candidates),
            questions=build_questions(candidates),
        )

    assert len(response.scores) == 5
    assert len(response.nouls) == 11  # 1 global + 2 per candidate
    with capsys.disabled():
        print(f"\n  5 candidates, 16 questions, 1 request — usage: {response.usage}")
