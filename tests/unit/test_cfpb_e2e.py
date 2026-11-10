"""End-to-end CFPB: local zip into CSV + site JSON via the real CLI."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from cam.__main__ import main
from cam.cfpb import window_months
from tests.unit.test_cfpb_runner import make_zip


def _rows_for(
    company: str, month: str, product: str, count: int, start_id: int
) -> list[tuple[str, str, str, str]]:
    y, m = int(month[:4]), int(month[5:7])
    received = f"{y}-{m:02d}-01"
    rows: list[tuple[str, str, str, str]] = []
    for i in range(count):
        rows.append((received, product, company, str(start_id + i)))
    return rows


def test_cli_cfpb_end_to_end_local_zip_no_network(tmp_path: Path) -> None:
    today = date.today()
    months = window_months(today, 24)
    assert len(months) == 24

    m0 = months[0]
    m5 = months[5]
    m10 = months[10]
    m23 = months[-1]

    expected: dict[str, dict[str, int]] = {
        "ACME": {m0: 2, m5: 3, m10: 1, m23: 4},
        "BETA": {m0: 1, m10: 3, m23: 4},
    }
    expected_totals = {k: sum(v.values()) for k, v in expected.items()}

    rows: list[tuple[str, str, str, str]] = []
    cid = 1
    rows.extend(_rows_for("ACME", m0, "Mortgage", 2, cid))
    cid += 2

    rows.extend(_rows_for("ACME", m5, "Mortgage", 1, cid))
    cid += 1
    rows.extend(_rows_for("ACME", m5, "Credit card", 2, cid))
    cid += 2

    rows.extend(_rows_for("ACME", m10, "Auto loan", 1, cid))
    cid += 1

    rows.extend(_rows_for("ACME", m23, "Mortgage", 3, cid))
    cid += 3
    rows.extend(_rows_for("ACME", m23, "Credit card", 1, cid))
    cid += 1

    rows.extend(_rows_for("BETA", m0, "Auto loan", 1, cid))
    cid += 1

    rows.extend(_rows_for("BETA", m10, "Auto loan", 2, cid))
    cid += 2
    rows.extend(_rows_for("BETA", m10, "Mortgage", 1, cid))
    cid += 1

    rows.extend(_rows_for("BETA", m23, "Credit card", 4, cid))

    zip_path = tmp_path / "bulk.zip"
    make_zip(zip_path, rows)

    data_dir = tmp_path / "data"
    site_dir = tmp_path / "site" / "data"
    args = [
        "cfpb",
        "--zip",
        str(zip_path),
        "--data-dir",
        str(data_dir),
        "--site-dir",
        str(site_dir),
        "--min-complaints",
        "1",
    ]

    rc1 = main(args)
    assert rc1 == 0
    assert zip_path.exists()

    monthly_path = data_dir / "cfpb" / "monthly.csv"
    assert monthly_path.exists()
    monthly_bytes_1 = monthly_path.read_bytes()
    assert monthly_bytes_1.startswith(b"company,month,product,complaints\n")
    assert len(monthly_bytes_1) > len(b"company,month,product,complaints\n")

    companies_path = site_dir / "cfpb_companies.json"
    meta_path = site_dir / "cfpb_meta.json"
    assert companies_path.exists()
    assert meta_path.exists()

    companies = json.loads(companies_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    assert meta["companies"] == 2
    assert meta["total_complaints"] == sum(expected_totals.values())
    assert meta["status"]["ok"] is True

    actual_by_company = {c["company"]: c for c in companies}
    assert set(actual_by_company) == {"ACME", "BETA"}

    for company, month_counts in expected.items():
        c = actual_by_company[company]
        assert c["total"] == expected_totals[company]
        assert len(c["months"]) == 24
        by_month = {m["month"]: m["complaints"] for m in c["months"]}
        for m in months:
            assert by_month[m] == month_counts.get(m, 0)

    rc2 = main(args)
    assert rc2 == 0
    monthly_bytes_2 = monthly_path.read_bytes()
    assert monthly_bytes_2 == monthly_bytes_1
