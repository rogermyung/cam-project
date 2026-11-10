"""Authoritative tests for ``cam.cfpb.build`` — CSV in, static-site JSON out."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def write_monthly(data: Path, rows: list[tuple[str, str, str, int]]) -> None:
    d = data / "cfpb"
    d.mkdir(parents=True, exist_ok=True)
    lines = ["company,month,product,complaints"]
    for company, month, product, n in rows:
        lines.append(f"{company},{month},{product},{n}")
    (d / "monthly.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_detect_spike_cases():
    from cam.cfpb.build import detect_spike

    assert detect_spike([10] * 12 + [16]) == {
        "latest": 16,
        "baseline": 10.0,
        "ratio": 1.6,
        "spike": True,
    }
    # Exactly +50% is not a spike.
    assert detect_spike([10] * 12 + [15])["spike"] is False
    # Zero baseline: ratio is None; spike only if there is at least one earlier month.
    assert detect_spike([0] * 12 + [3]) == {
        "latest": 3,
        "baseline": 0.0,
        "ratio": None,
        "spike": True,
    }
    assert detect_spike([0] * 12 + [0])["spike"] is False
    assert detect_spike([7]) == {
        "latest": 7,
        "baseline": 0.0,
        "ratio": None,
        "spike": False,
    }


def test_build_zero_fills_missing_months_and_orders(tmp_path: Path):
    from cam.cfpb.build import build

    data, site = tmp_path / "data", tmp_path / "site"
    write_monthly(
        data,
        [
            ("B CO", "2026-01", "Mortgage", 2),
            ("A CO", "2026-01", "Alpha", 1),
            ("A CO", "2026-03", "Mortgage", 4),
        ],
    )
    build(data, site, now=NOW)
    companies = json.loads((site / "cfpb_companies.json").read_text(encoding="utf-8"))
    assert [c["company"] for c in companies] == ["A CO", "B CO"]
    a = next(c for c in companies if c["company"] == "A CO")
    assert [m["month"] for m in a["months"]] == ["2026-01", "2026-02", "2026-03"]
    assert [m["complaints"] for m in a["months"]] == [1, 0, 4]


def test_build_sort_orders_and_top_product(tmp_path: Path):
    from cam.cfpb.build import build

    data, site = tmp_path / "data", tmp_path / "site"
    write_monthly(
        data,
        [
            ("C CO", "2026-10", "Mortgage", 5),
            ("C CO", "2026-10", "Alpha", 5),
            ("A CO", "2026-08", "Mortgage", 2),
            ("A CO", "2026-09", "Mortgage", 1),
            ("A CO", "2026-10", "Mortgage", 2),
            ("B CO", "2026-08", "Mortgage", 1),
            ("B CO", "2026-09", "Mortgage", 2),
            ("B CO", "2026-10", "Mortgage", 2),
        ],
    )
    build(data, site, now=NOW)
    companies = json.loads((site / "cfpb_companies.json").read_text(encoding="utf-8"))
    assert [c["company"] for c in companies] == ["C CO", "A CO", "B CO"]

    c = next(c for c in companies if c["company"] == "C CO")
    assert c["top_product"] == "Alpha"  # tie: name ascending
    assert [p["product"] for p in c["products"]] == ["Alpha", "Mortgage"]


def test_build_status_pass_through(tmp_path: Path):
    from cam.cfpb.build import build

    data, site = tmp_path / "data", tmp_path / "site"
    write_monthly(data, [("A CO", "2026-01", "Mortgage", 1)])
    (data / "cfpb" / "status.json").write_text(
        json.dumps({"ok": True, "note": "hello"}), encoding="utf-8"
    )
    meta = build(data, site, now=NOW)
    assert meta["status"] == {"ok": True, "note": "hello"}
    meta_file = json.loads((site / "cfpb_meta.json").read_text(encoding="utf-8"))
    assert meta_file["status"] == meta["status"]


def test_build_refuses_empty_or_missing_monthly(tmp_path: Path):
    from cam.cfpb.build import build

    with pytest.raises(ValueError):
        build(tmp_path / "data", tmp_path / "site", now=NOW)
