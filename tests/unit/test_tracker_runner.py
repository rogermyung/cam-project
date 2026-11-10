"""Authoritative tests for ``cam.warn.runner`` and the ``python -m cam warn`` CLI."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from cam.store import load
from cam.warn import FIELDNAMES
from cam.warn.runner import RunFailed, run
from cam.warn.state_urls import STATE_CONFIGS

FIXTURES = Path(__file__).parent.parent / "fixtures" / "warn"
TODAY = date(2026, 11, 9)
EMPTY_PAGE = b"<html><body><p>no table here</p></body></html>"
TX_HEADER = b'"notice_date","job_site_name","county_name","wda_name","total_layoff_number","layoff_date","wfdd_received_date","city_name"\n'


def make_fetcher(overrides: dict[str, object] | None = None):
    """Fake fetcher: CA=9 rows, MI=6, TX=8; NY and PA return a page with no notices."""
    payloads: dict[str, object] = {
        "CA": (FIXTURES / "ca_warn_report.xlsx").read_bytes(),
        "MI": (FIXTURES / "mi_warn_sample.json").read_bytes(),
        "TX": (FIXTURES / "tx_socrata_sample.csv").read_bytes(),
        "NY": EMPTY_PAGE,
        "PA": EMPTY_PAGE,
    }
    payloads.update(overrides or {})
    by_url = {STATE_CONFIGS[code].url: code for code in STATE_CONFIGS}
    calls: list[str] = []

    def fetcher(url: str) -> bytes:
        code = by_url[url]
        calls.append(code)
        payload = payloads[code]
        if isinstance(payload, Exception):
            raise payload
        return payload  # type: ignore[return-value]

    fetcher.calls = calls  # type: ignore[attr-defined]
    return fetcher


def notices(data_dir: Path) -> list[dict[str, str]]:
    return load(data_dir / "warn" / "notices.csv")


def status(data_dir: Path) -> dict:
    return json.loads((data_dir / "warn" / "status.json").read_text())


# --- happy path -----------------------------------------------------------------


def test_run_writes_notices_csv_with_schema_and_counts(tmp_path):
    report = run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    rows = notices(tmp_path)
    assert len(rows) == 23
    assert list(rows[0]) == FIELDNAMES
    assert {r["state"] for r in rows} == {"CA", "MI", "TX"}
    assert all(r["first_seen"] == "2026-11-09" for r in rows)
    assert report["total"] == 23
    assert report["added"] == 23


def test_run_records_source_url_per_state(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    for row in notices(tmp_path):
        assert row["source_url"] == STATE_CONFIGS[row["state"]].url


def test_rows_are_sorted_by_state_then_notice_date_then_key(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    rows = notices(tmp_path)
    assert rows == sorted(rows, key=lambda r: (r["state"], r["notice_date"], r["key"]))


def test_second_run_adds_nothing_and_leaves_csv_byte_identical(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    before = (tmp_path / "warn" / "notices.csv").read_bytes()
    report = run(tmp_path, fetcher=make_fetcher(), today=date(2026, 11, 16))
    assert report["added"] == 0
    assert report["total"] == 23
    assert (tmp_path / "warn" / "notices.csv").read_bytes() == before


def test_records_with_blank_company_are_skipped(tmp_path):
    html = (
        TX_HEADER
        + b'"2026-01-02T00:00:00.000","  ","Travis","X","10","","","Austin"\n'
        + b'"2026-01-02T00:00:00.000","Real Co","Travis","X","10","","","Austin"\n'
    )
    run(tmp_path, states=["TX"], fetcher=make_fetcher({"TX": html}), today=TODAY)
    assert [r["employer"] for r in notices(tmp_path)] == ["Real Co"]


# --- per-state status -----------------------------------------------------------


def test_status_json_reports_each_state(tmp_path):
    report = run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    st = status(tmp_path)
    assert set(st) == {"CA", "TX", "NY", "PA", "MI"}
    assert st["CA"] == {
        "ok": True,
        "fetched": 9,
        "added": 9,
        "error": None,
        "last_attempt": "2026-11-09",
        "last_success": "2026-11-09",
        "degraded": False,
    }
    assert st["MI"]["fetched"] == 6
    assert st["TX"]["fetched"] == 8
    assert report["states"] == st


def test_page_that_parses_to_zero_records_is_a_failure_for_that_state(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    ny = status(tmp_path)["NY"]
    assert ny["ok"] is False
    assert ny["fetched"] == 0
    assert ny["added"] == 0
    assert ny["last_success"] is None
    assert "0 records" in ny["error"]


def test_one_state_raising_does_not_stop_the_others(tmp_path):
    boom = httpx.ConnectError("connection refused")
    report = run(tmp_path, fetcher=make_fetcher({"CA": boom}), today=TODAY)
    st = status(tmp_path)
    assert st["CA"]["ok"] is False
    assert "connection refused" in st["CA"]["error"]
    assert st["TX"]["ok"] is True
    assert report["total"] == 14


def test_failed_state_keeps_previous_rows_last_success_and_fetched(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    later = date(2026, 11, 16)
    run(tmp_path, fetcher=make_fetcher({"CA": RuntimeError("HTTP 503")}), today=later)
    ca = status(tmp_path)["CA"]
    assert ca["ok"] is False
    assert ca["last_attempt"] == "2026-11-16"
    assert ca["last_success"] == "2026-11-09"
    assert ca["fetched"] == 9
    assert ca["added"] == 0
    assert sum(1 for r in notices(tmp_path) if r["state"] == "CA") == 9


def test_state_is_degraded_when_fetch_count_drops_below_half(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    one_row = TX_HEADER + b'"2026-02-02T00:00:00.000","Solo Co","McLennan","X","5","","","Waco"\n'
    run(tmp_path, fetcher=make_fetcher({"TX": one_row}), today=date(2026, 11, 16))
    st = status(tmp_path)
    assert st["TX"]["ok"] is True
    assert st["TX"]["fetched"] == 1
    assert st["TX"]["degraded"] is True
    assert st["CA"]["degraded"] is False


def test_states_filter_only_fetches_those_states_and_preserves_other_status(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    fetcher = make_fetcher()
    run(tmp_path, states=["TX"], fetcher=fetcher, today=date(2026, 11, 16))
    assert fetcher.calls == ["TX"]
    st = status(tmp_path)
    assert st["TX"]["last_attempt"] == "2026-11-16"
    assert st["CA"]["last_attempt"] == "2026-11-09"
    assert st["CA"]["fetched"] == 9


def test_unknown_state_code_raises_value_error(tmp_path):
    with pytest.raises(ValueError):
        run(tmp_path, states=["ZZ"], fetcher=make_fetcher(), today=TODAY)


def test_status_json_is_sorted_indented_and_newline_terminated(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    text = (tmp_path / "warn" / "status.json").read_text()
    assert text == json.dumps(json.loads(text), indent=2, sort_keys=True) + "\n"


# --- guardrails -----------------------------------------------------------------


def test_run_fails_loudly_when_every_state_fails(tmp_path):
    run(tmp_path, fetcher=make_fetcher(), today=TODAY)
    boom = RuntimeError("down")
    fetcher = make_fetcher({code: boom for code in STATE_CONFIGS})
    with pytest.raises(RunFailed):
        run(tmp_path, fetcher=fetcher, today=date(2026, 11, 16))
    # status is still written so the failure is visible, and old rows survive
    assert status(tmp_path)["CA"]["last_attempt"] == "2026-11-16"
    assert len(notices(tmp_path)) == 23


def test_run_fails_loudly_on_a_fresh_directory_with_no_data(tmp_path):
    fetcher = make_fetcher({code: EMPTY_PAGE for code in ("TX", "NY", "PA")})
    with pytest.raises(RunFailed):
        run(tmp_path, states=["TX", "NY", "PA"], fetcher=fetcher, today=TODAY)


def test_run_failed_is_a_runtime_error():
    assert issubclass(RunFailed, RuntimeError)


# --- CLI ------------------------------------------------------------------------


def test_cli_warn_runs_and_returns_zero(tmp_path, monkeypatch, capsys):
    from cam.__main__ import main

    monkeypatch.setattr("cam.warn.runner.fetch", make_fetcher())
    code = main(["warn", "--data-dir", str(tmp_path), "--no-build"])
    assert code == 0
    assert len(notices(tmp_path)) == 23
    out = capsys.readouterr().out
    for state in ("CA", "TX", "NY", "PA", "MI"):
        assert state in out
    assert "23" in out


def test_cli_states_option_limits_the_run(tmp_path, monkeypatch):
    from cam.__main__ import main

    fetcher = make_fetcher()
    monkeypatch.setattr("cam.warn.runner.fetch", fetcher)
    assert main(["warn", "--data-dir", str(tmp_path), "--states", "tx, ca", "--no-build"]) == 0
    assert sorted(fetcher.calls) == ["CA", "TX"]


def test_cli_returns_one_and_reports_on_stderr_when_run_fails(tmp_path, monkeypatch, capsys):
    from cam.__main__ import main

    boom = RuntimeError("down")
    monkeypatch.setattr(
        "cam.warn.runner.fetch", make_fetcher({code: boom for code in STATE_CONFIGS})
    )
    assert main(["warn", "--data-dir", str(tmp_path), "--no-build"]) == 1
    assert capsys.readouterr().err.strip() != ""


def test_cli_without_no_build_calls_build_with_both_directories(tmp_path, monkeypatch):
    import sys
    import types

    from cam.__main__ import main

    seen: dict[str, Path] = {}
    fake = types.ModuleType("cam.warn.build")

    def build(data_dir, site_dir, **kwargs):
        seen["data"] = Path(data_dir)
        seen["site"] = Path(site_dir)
        return {}

    fake.build = build  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "cam.warn.build", fake)
    monkeypatch.setattr("cam.warn.runner.fetch", make_fetcher())
    site = tmp_path / "site-data"
    assert main(["warn", "--data-dir", str(tmp_path / "d"), "--site-dir", str(site)]) == 0
    assert seen == {"data": tmp_path / "d", "site": site}
