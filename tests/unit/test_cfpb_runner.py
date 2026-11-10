from __future__ import annotations

import csv
import io
import json
import zipfile
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


def run_with(zip_path: Path, data: Path, *, today: date, **kw):
    from cam.cfpb import runner

    kw.setdefault("today", today)
    return runner.run(data, downloader=lambda _url: zip_path, **kw)


def test_run_is_deterministic_and_replaces_monthly_csv(tmp_path: Path):

    today = date(2026, 11, 10)
    z = make_zip(
        tmp_path / "bulk.zip",
        [
            ("2026-10-05", "Mortgage", "ACME", "1"),
            ("2026-10-06", "Mortgage", "ACME", "2"),
            ("2026-09-15", "Credit card", "ACME", "3"),
            ("2026-10-07", "Mortgage", "OTHER", "4"),
        ],
    )

    data = tmp_path / "data"
    status1 = run_with(z, data, today=today, window=3, min_complaints=1)
    monthly1 = (data / "cfpb" / "monthly.csv").read_bytes()
    status_json1 = (data / "cfpb" / "status.json").read_bytes()
    status2 = run_with(z, data, today=today, window=3, min_complaints=1)
    monthly2 = (data / "cfpb" / "monthly.csv").read_bytes()
    status_json2 = (data / "cfpb" / "status.json").read_bytes()

    assert status1 == status2
    assert monthly1 == monthly2
    assert status_json1 == status_json2
    assert b"company,month,product,complaints\n" in monthly1


def test_zero_rows_after_filter_is_failure_and_keeps_previous_file(tmp_path: Path):
    from cam.cfpb import runner

    today = date(2026, 11, 10)
    later = date(2026, 11, 17)

    z = make_zip(
        tmp_path / "bulk.zip",
        [
            ("2026-10-05", "Mortgage", "ACME", "1"),
            ("2026-10-06", "Mortgage", "ACME", "2"),
            ("2026-09-15", "Credit card", "ACME", "3"),
            ("2026-10-07", "Mortgage", "OTHER", "4"),
        ],
    )
    data = tmp_path / "data"
    run_with(z, data, today=today, window=3, min_complaints=1)
    monthly_before = (data / "cfpb" / "monthly.csv").read_bytes()
    status_before = json.loads((data / "cfpb" / "status.json").read_text())

    with pytest.raises(runner.RunFailed):
        run_with(z, data, today=later, window=3, min_complaints=10_000)

    assert (data / "cfpb" / "monthly.csv").read_bytes() == monthly_before
    st = json.loads((data / "cfpb" / "status.json").read_text())
    assert st["ok"] is False
    assert st["last_attempt"] == later.isoformat()
    assert st["last_success"] == status_before["last_success"]
    assert st["companies"] == status_before["companies"]
    assert st["rows"] == status_before["rows"]


def test_cli_cfpb_with_local_zip_uses_zip_without_deleting(tmp_path: Path, capsys):
    from cam.__main__ import main
    from cam.cfpb import window_months

    today = date.today()
    months = window_months(today, 24)
    in_month = months[-1]
    year, month = int(in_month[:4]), int(in_month[5:7])
    received = f"{year}-{month:02d}-01"

    z = make_zip(
        tmp_path / "bulk.zip",
        [
            (received, "Mortgage", "ACME", "1"),
            (received, "", "ACME", "2"),
        ],
    )

    data_dir = tmp_path / "data"
    rc = main(
        [
            "cfpb",
            "--data-dir",
            str(data_dir),
            "--zip",
            str(z),
            "--min-complaints",
            "1",
            "--no-build",
        ]
    )
    assert rc == 0
    assert z.exists()
    assert (data_dir / "cfpb" / "monthly.csv").exists()
    assert "companies" in capsys.readouterr().out.lower()
