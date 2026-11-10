from __future__ import annotations

import csv
import io
import zipfile
from collections import Counter
from datetime import date
from pathlib import Path

import pytest

HEADER = [
    "Date received",
    "Product",
    "Sub-product",
    "Issue",
    "Sub-issue",
    "Consumer complaint narrative",
    "Company public response",
    "Company",
    "State",
    "ZIP code",
    "Tags",
    "Consumer consent provided?",
    "Submitted via",
    "Date sent to company",
    "Company response to consumer",
    "Timely response?",
    "Consumer disputed?",
    "Complaint ID",
]


def make_zip(path: Path, rows: list[tuple[str, str, str, str]], *, bom: bool = True) -> Path:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(HEADER)
    for received, product, company, cid in rows:
        rec = dict.fromkeys(HEADER, "")
        rec.update(
            {
                "Date received": received,
                "Product": product,
                "Company": company,
                "Complaint ID": cid,
            }
        )
        w.writerow([rec[h] for h in HEADER])
    data = buf.getvalue().encode("utf-8-sig" if bom else "utf-8")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("complaints.csv", data)
    return path


def test_window_months_cross_year_boundary():
    from cam.cfpb import window_months

    today = date(2026, 1, 15)
    assert window_months(today, 3) == ["2025-10", "2025-11", "2025-12"]


def test_parse_date_and_clean_company_name():
    from cam.cfpb import clean_company_name, parse_date

    assert parse_date("2026-10-03") == date(2026, 10, 3)
    assert parse_date("10/03/2026") == date(2026, 10, 3)
    assert parse_date("2026-10-03T12:00:00-05:00") == date(2026, 10, 3)
    assert parse_date("") is None
    assert parse_date("not a date") is None

    assert clean_company_name("  BIG   BANK,  N.A. ") == "BIG BANK, N.A."
    assert clean_company_name(None) == ""


def test_aggregate_skip_rules_and_rows_read_count(tmp_path: Path):
    from cam.cfpb import aggregate, window_months

    today = date(2026, 11, 10)
    months = window_months(today, 3)

    z = make_zip(
        tmp_path / "bulk.zip",
        [
            ("2026-10-05", "Mortgage", "ACME", ""),  # blank complaint id
            ("not a date", "Mortgage", "ACME", "1"),  # bad date
            ("2026-07-01", "Mortgage", "ACME", "2"),  # outside months
            ("2026-10-05", "Mortgage", "   ", "3"),  # empty company
            ("2026-10-07", "", "ACME", "4"),  # blank product => Unknown
            ("2026-09-15", "Credit card", "ACME", "5"),
        ],
    )

    counts, rows_read = aggregate(z, months=months)
    assert rows_read == 6
    assert dict(counts) == {
        ("ACME", "2026-10", "Unknown"): 1,
        ("ACME", "2026-09", "Credit card"): 1,
    }


def test_to_rows_minimum_volume_filter_and_field_order():
    from cam.cfpb import FIELDNAMES, to_rows

    counts = Counter(
        {
            ("A", "2026-10", "Mortgage"): 2,
            ("A", "2026-09", "Credit card"): 1,
            ("B", "2026-10", "Mortgage"): 4,
        }
    )

    rows = to_rows(counts, min_complaints=2)
    assert {r["company"] for r in rows} == {"A", "B"}

    rows_b = to_rows(counts, min_complaints=5)
    assert rows_b == []

    one = to_rows(counts, min_complaints=4)
    assert len(one) == 1
    assert one[0]["company"] == "B"
    assert list(one[0].keys()) == FIELDNAMES


def test_aggregate_rejects_zip_without_csv_member(tmp_path: Path):
    from cam.cfpb import aggregate

    z = tmp_path / "empty.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("readme.txt", "nothing")

    with pytest.raises(ValueError):
        aggregate(z, months=["2026-10"])
