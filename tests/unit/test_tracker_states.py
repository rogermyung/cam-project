"""Authoritative tests for the Texas, New York and Pennsylvania sources.

Fixtures are trimmed captures of the live sources taken on 2026-11-09:
- tx_socrata_sample.csv  — Texas Open Data Portal CSV (dataset 8w53-c4f6)
- ny_tableau_sample.csv  — New York Tableau Public dashboard CSV export
- pa_warn_notices.html   — Pennsylvania accordion listing page
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from cam.warn import WarnRecord, parse, record_key
from cam.warn._pa import parse_pa
from cam.warn.state_urls import STATE_CONFIGS, StateConfig

FIXTURES = Path(__file__).parent.parent / "fixtures" / "warn"


def _squash(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


# --- Texas (config only: generic CSV parser) ------------------------------------


def test_texas_parses_open_data_csv():
    records = parse((FIXTURES / "tx_socrata_sample.csv").read_bytes(), STATE_CONFIGS["TX"])
    assert len(records) == 8
    first = records[0]
    assert first.state_code == "TX"
    assert first.company == "Amentum"
    assert first.notice_date == date(2022, 1, 28)
    assert first.employees_affected == 178
    assert first.city == "New Boston"
    assert first.county == "Bowie"
    assert first.layoff_type == ""
    assert records[-1].company == "Fortrex (John Soules Foods, Inc.)"
    assert records[-1].notice_date == date(2026, 6, 12)
    assert all(r.notice_date is not None for r in records)


# --- New York (config + whitespace-tolerant CSV headers) ------------------------


def test_new_york_parses_tableau_export_despite_trailing_spaces_in_headers():
    records = parse((FIXTURES / "ny_tableau_sample.csv").read_bytes(), STATE_CONFIGS["NY"])
    assert len(records) == 6
    first = records[0]
    assert first.state_code == "NY"
    assert first.company == "420 Park FB LLC"
    assert first.notice_date == date(2026, 4, 6)
    assert first.employees_affected == 42
    assert _squash(first.city) == "420 Park Ave S New York, NY, 10016"
    assert first.county == "New York"
    assert first.layoff_type == "Closure"
    assert all(r.notice_date is not None and r.employees_affected for r in records)


def test_new_york_sites_of_one_employer_get_distinct_keys():
    records = parse((FIXTURES / "ny_tableau_sample.csv").read_bytes(), STATE_CONFIGS["NY"])
    air = [r for r in records if r.company == "Air Company Holdings Inc."]
    assert [r.employees_affected for r in air] == [4, 14, 5]
    assert len({record_key(r) for r in air}) == 3


def test_csv_header_matching_ignores_case_and_surrounding_whitespace():
    cfg = StateConfig(
        state_code="ZZ",
        url="https://example.test",
        format="csv",
        columns={"company": "employer name", "date": "Notice Date", "employees": "WORKERS"},
        date_fmt="%Y-%m-%d",
    )
    content = b"  Employer Name ,notice date  , Workers\nAcme Corp,2026-03-04,12\n"
    (record,) = parse(content, cfg)
    assert record.company == "Acme Corp"
    assert record.notice_date == date(2026, 3, 4)
    assert record.employees_affected == 12
    # raw keeps the source headers exactly as published
    assert record.raw["  Employer Name "] == "Acme Corp"


# --- Pennsylvania (new accordion parser) ----------------------------------------


@pytest.fixture(scope="module")
def pa() -> dict[str, WarnRecord]:
    records = parse_pa((FIXTURES / "pa_warn_notices.html").read_bytes())
    assert len(records) == 9, [r.company for r in records]
    by_name = {r.company: r for r in records}
    assert len(by_name) == 9
    return by_name


def test_pa_dispatches_through_parse():
    records = parse((FIXTURES / "pa_warn_notices.html").read_bytes(), STATE_CONFIGS["PA"])
    assert len(records) == 9
    assert all(isinstance(r, WarnRecord) and r.state_code == "PA" for r in records)


def test_pa_month_headings_are_not_records(pa):
    months = {"January", "February", "March", "April", "May", "June", "July", "August"}
    months |= {"September", "October", "November", "December"}
    assert not months & set(pa)


def test_pa_standard_notice(pa):
    rec = pa["Comcast Technology Center"]
    assert rec.county == "Philadelphia"
    assert rec.employees_affected == 634
    assert rec.notice_date == date(2026, 12, 31)  # first date of "beginning ...; ending ..."
    assert rec.layoff_type == "Layoff"  # trailing zero-width space removed
    assert _squash(rec.city) == "1800 Arch Street, Philadelphia, PA 19103"
    assert "EFFECTIVE DATE" in rec.raw["text"]
    assert rec.raw["title"] == "Comcast Technology Center"


def test_pa_employee_count_with_trailing_note(pa):
    rec = pa["Family Endeavors, Inc"]
    assert rec.employees_affected == 15
    assert rec.county == "Various"
    assert rec.notice_date == date(2026, 12, 31)


def test_pa_two_digit_year(pa):
    rec = pa["CJ Logistics America, LLC"]
    assert rec.notice_date == date(2026, 10, 31)
    assert rec.employees_affected == 56
    assert rec.layoff_type == "Closure"


def test_pa_date_range_uses_the_first_date(pa):
    assert pa["Pep Boys Manny Moe & Jack, LLC"].notice_date == date(2026, 12, 31)
    assert pa["Jerith Manufacturing, LLC"].notice_date == date(2026, 11, 15)


def test_pa_affected_label_without_hash(pa):
    rec = pa["First Brands Group, LLC, d/b/a Eagle Castings"]
    assert rec.employees_affected == 73
    assert rec.notice_date == date(2026, 2, 27)
    assert rec.layoff_type == "Closing"


def test_pa_plural_labels(pa):
    rec = pa["Carson Valley Children’s Aid"]
    assert rec.county == "Montgomery and Philadelphia"  # "COUNTIES:"
    assert rec.employees_affected == 96
    assert rec.notice_date == date(2025, 9, 1)  # "EFFECTIVE DATES:"
    assert pa["PrimeCare Medical, Inc."].county == "York"
    assert pa["PrimeCare Medical, Inc."].employees_affected == 50


def test_pa_unlabelled_notice_falls_back_gracefully(pa):
    rec = pa["Everett Foodliner, Inc. and Saxton Market, Inc."]
    assert rec.county == "Bedford"  # "County:" in mixed case
    assert rec.employees_affected is None  # no AFFECTED line
    assert rec.notice_date == date(2026, 4, 20)  # no EFFECTIVE line: first date in the text
    assert rec.layoff_type == ""


def test_pa_page_without_accordions_returns_empty_list():
    assert parse_pa(b"<html><body><p>Page moved</p></body></html>") == []
