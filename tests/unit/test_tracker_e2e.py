"""End-to-end: fixtures in, CSV and site JSON out, through the real CLI."""

from __future__ import annotations

import json

from cam.__main__ import main
from tests.unit.test_tracker_runner import make_fetcher


def test_cli_fetch_merge_build_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setattr("cam.warn.runner.fetch", make_fetcher())
    data, site = tmp_path / "data", tmp_path / "site" / "data"
    args = ["warn", "--data-dir", str(data), "--site-dir", str(site)]

    assert main(args) == 0
    csv_first = (data / "warn" / "notices.csv").read_bytes()
    notices = json.loads((site / "notices.json").read_text())
    summary = json.loads((site / "summary.json").read_text())
    meta = json.loads((site / "meta.json").read_text())

    assert len(notices) == 23
    assert meta["total_notices"] == 23
    assert {s["state"] for s in summary["by_state"]} == {"CA", "MI", "TX"}
    assert meta["states"]["NY"]["ok"] is False

    assert main(args) == 0
    assert (data / "warn" / "notices.csv").read_bytes() == csv_first
