"""Authoritative tests for ``cam.warn.build`` — CSV in, static-site JSON out."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cam.store import save
from cam.warn import FIELDNAMES
from cam.warn.build import build

NOW = datetime(2026, 11, 9, 12, 0, 0, tzinfo=UTC)


def _row(state, employer, notice_date, employees, city="Town", notice_type="Layoff"):
    return {
        "key": f"{state}::{employer.lower()}::{notice_date or 'no-date:abc'}::{city.lower()}",
        "state": state,
        "employer": employer,
        "notice_date": notice_date,
        "employees": employees,
        "city": city,
        "county": "Some County",
        "notice_type": notice_type,
        "source_url": f"https://example.test/{state}",
        "first_seen": "2026-11-09",
    }


ROWS = [
    _row("CA", "Alpha Inc", "2026-06-30", "200"),
    _row("CA", "Alpha Inc", "2026-07-15", "50", city="Other"),
    _row("CA", "Beta LLC", "2026-07-01", "1000"),
    _row("TX", "ALPHA INC", "2026-07-20", "30"),
    _row("TX", "Gamma Co", "2026-05-05", ""),
    _row("TX", "Delta Co", "", "75"),
    _row("MI", "gamma co ", "2026-05-06", "10"),
    _row("MI", "Gamma Co", "2026-05-07", "20", city="Elsewhere"),
]

STATUS = {
    "CA": {"ok": True, "fetched": 3, "last_success": "2026-11-09"},
    "TX": {"ok": False, "fetched": 3, "last_success": "2026-11-02", "error": "HTTP 503"},
}


@pytest.fixture
def dirs(tmp_path):
    data, site = tmp_path / "data", tmp_path / "site" / "data"
    save(data / "warn" / "notices.csv", ROWS, FIELDNAMES, sort_by=["state", "notice_date", "key"])
    (data / "warn" / "status.json").write_text(json.dumps(STATUS))
    return data, site


def _json(site: Path, name: str):
    return json.loads((site / name).read_text(encoding="utf-8"))


def test_build_creates_the_three_files(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    assert sorted(p.name for p in site.iterdir()) == ["meta.json", "notices.json", "summary.json"]


def test_notices_json_types_and_order(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    notices = _json(site, "notices.json")
    assert len(notices) == 8
    assert list(notices[0]) == FIELDNAMES
    # newest first, undated last
    assert [n["notice_date"] for n in notices] == [
        "2026-07-20",
        "2026-07-15",
        "2026-07-01",
        "2026-06-30",
        "2026-05-07",
        "2026-05-06",
        "2026-05-05",
        None,
    ]
    assert notices[0]["employees"] == 30
    assert notices[6]["employees"] is None
    assert notices[7]["employer"] == "Delta Co"
    assert notices[7]["employees"] == 75


def test_summary_by_state(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    assert _json(site, "summary.json")["by_state"] == [
        {"state": "CA", "notices": 3, "employees": 1250},
        {"state": "MI", "notices": 2, "employees": 30},
        {"state": "TX", "notices": 3, "employees": 105},
    ]


def test_summary_by_month_is_ascending_and_excludes_undated(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    assert _json(site, "summary.json")["by_month"] == [
        {"month": "2026-05", "notices": 3, "employees": 30},
        {"month": "2026-06", "notices": 1, "employees": 200},
        {"month": "2026-07", "notices": 3, "employees": 1080},
    ]


def test_summary_largest_orders_by_employees_and_skips_unknown_counts(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    largest = _json(site, "summary.json")["largest"]
    assert [n["employees"] for n in largest] == [1000, 200, 75, 50, 30, 20, 10]
    assert largest[0]["employer"] == "Beta LLC"
    assert list(largest[0]) == FIELDNAMES


def test_summary_largest_is_capped_at_twenty(tmp_path):
    data, site = tmp_path / "data", tmp_path / "site"
    rows = [_row("TX", f"Co {i:02d}", "2026-01-01", str(i + 1), city=f"C{i}") for i in range(25)]
    save(data / "warn" / "notices.csv", rows, FIELDNAMES, sort_by=["key"])
    build(data, site, now=NOW)
    largest = _json(site, "summary.json")["largest"]
    assert len(largest) == 20
    assert largest[0]["employees"] == 25


def test_summary_repeat_employers_groups_within_a_state_case_insensitively(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    repeats = _json(site, "summary.json")["repeat_employers"]
    # Alpha Inc appears twice in CA (TX's single ALPHA INC is a different group);
    # Gamma Co appears twice in MI under different casing/whitespace.
    assert repeats == [
        {"state": "CA", "employer": "Alpha Inc", "notices": 2, "employees": 250},
        {"state": "MI", "employer": "Gamma Co", "notices": 2, "employees": 30},
    ]


def test_meta_json(dirs):
    data, site = dirs
    meta = build(data, site, now=NOW)
    assert meta == _json(site, "meta.json")
    assert meta == {
        "generated_at": "2026-11-09T12:00:00+00:00",
        "total_notices": 8,
        "total_employees": 1385,
        "latest_notice_date": "2026-07-20",
        "states": STATUS,
    }


def test_missing_status_file_gives_empty_states(dirs):
    data, site = dirs
    (data / "warn" / "status.json").unlink()
    assert build(data, site, now=NOW)["states"] == {}


def test_output_is_deterministic_and_newline_terminated(dirs):
    data, site = dirs
    build(data, site, now=NOW)
    first = {p.name: p.read_bytes() for p in site.iterdir()}
    build(data, site, now=NOW)
    assert {p.name: p.read_bytes() for p in site.iterdir()} == first
    assert all(blob.endswith(b"\n") for blob in first.values())


def test_non_ascii_text_is_written_as_utf8_not_escaped(tmp_path):
    data, site = tmp_path / "data", tmp_path / "site"
    save(
        data / "warn" / "notices.csv",
        [_row("CA", "Café Niño", "2026-01-01", "5")],
        FIELDNAMES,
        sort_by=["key"],
    )
    build(data, site, now=NOW)
    assert "Café Niño" in (site / "notices.json").read_text(encoding="utf-8")


def test_build_refuses_to_publish_an_empty_dataset(tmp_path):
    with pytest.raises(ValueError):
        build(tmp_path / "data", tmp_path / "site", now=NOW)
    assert not (tmp_path / "site").exists()
