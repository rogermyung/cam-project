"""
Authoritative tests for the Michigan WARN parser (M11 — Phase 4).

Michigan retired the old ``.../wd/warn`` HTML page (now a 404) in 2026.  The
current notices live at ``.../wd/data-public-notices/warn-notices``, which is a
JavaScript shell backed by a Sitecore search API that returns a JSON envelope::

    {"Count": N, "Results": [{"Html": "<div>...<h3>Company</h3><ul>...</ul>"}, ...]}

Each ``Results[i].Html`` is an HTML fragment with the company in an ``<h3>`` and
the notice fields in ``<li><strong>Label:</strong> value</li>`` pairs.  Some
``Layoff date`` values are ranges (``"8/15/2026 – 12/31/2026"``); the parser
must take the first date.

``parse_mi(content: bytes) -> list[WarnRecord]`` parses that JSON envelope
(as returned by the search API) into WarnRecords.  Assertions are pinned to the
committed fixture ``tests/fixtures/warn/mi_warn_sample.json`` (a trimmed capture
of the real API response).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from cam.ingestion.warn import WarnRecord
from cam.ingestion.warn._mi import parse_mi

FIXTURE = Path(__file__).parent.parent / "fixtures" / "warn" / "mi_warn_sample.json"


def _load() -> list[WarnRecord]:
    return parse_mi(FIXTURE.read_bytes())


def test_returns_all_six_records():
    assert len(_load()) == 6


def test_returns_warnrecord_instances_tagged_mi():
    for rec in _load():
        assert isinstance(rec, WarnRecord)
        assert rec.state_code == "MI"


def test_every_record_has_nonempty_company():
    for rec in _load():
        assert rec.company, f"empty company in {rec!r}"


def test_first_record_fields():
    rec = _load()[0]
    assert rec.company == "Conduent Commercial Solutions, LLC"
    assert rec.employees_affected == 15
    assert rec.notice_date == date(2026, 8, 28)
    assert "Layoff" in rec.layoff_type
    assert "Statewide" in rec.county


def test_range_date_takes_first_date():
    # Rec Boat Holdings has "Layoff date: 8/15/2026 – 12/31/2026" — take the first.
    recs = {r.company: r for r in _load()}
    rec = recs["Rec Boat Holdings, LLC"]
    assert rec.notice_date == date(2026, 8, 15)
    assert rec.employees_affected == 239
    assert "closure" in rec.layoff_type.lower()


def test_all_employee_counts_are_ints():
    counts = [r.employees_affected for r in _load()]
    assert counts == [15, 239, 68, 82, 94, 49]
    assert all(isinstance(c, int) for c in counts)


def test_all_notice_dates_parsed():
    for rec in _load():
        assert isinstance(rec.notice_date, date)
