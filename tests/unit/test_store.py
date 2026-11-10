"""Authoritative tests for ``cam.store`` — the flat-file replacement for the database."""

from __future__ import annotations

import subprocess
import sys

from cam.store import load, merge, save

FIELDS = ["key", "state", "employer", "first_seen"]


def _row(key: str, state: str = "TX", employer: str = "Acme", first_seen: str = "2026-01-01"):
    return {"key": key, "state": state, "employer": employer, "first_seen": first_seen}


def test_store_uses_only_the_standard_library():
    code = (
        "import sys\n"
        "before = set(sys.modules)\n"
        "import cam.store\n"
        "new = set(sys.modules) - before\n"
        "third = sorted(m for m in new if m.split('.')[0] in "
        "{'sqlalchemy', 'pandas', 'numpy', 'pydantic', 'httpx'} or m.startswith('cam.') "
        "and m != 'cam.store')\n"
        "assert not third, third\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


def test_load_missing_file_returns_empty_list(tmp_path):
    assert load(tmp_path / "nope.csv") == []


def test_save_then_load_round_trips(tmp_path):
    path = tmp_path / "out.csv"
    rows = [_row("a"), _row("b", employer='Quote "Co", Inc')]
    save(path, rows, FIELDS, sort_by=["key"])
    assert load(path) == rows


def test_save_creates_parent_directories(tmp_path):
    path = tmp_path / "data" / "warn" / "notices.csv"
    save(path, [_row("a")], FIELDS, sort_by=["key"])
    assert path.exists()


def test_save_writes_header_first_and_unix_newlines(tmp_path):
    path = tmp_path / "out.csv"
    save(path, [_row("a")], FIELDS, sort_by=["key"])
    raw = path.read_bytes()
    assert raw.startswith(b"key,state,employer,first_seen\n")
    assert b"\r" not in raw
    assert raw.endswith(b"\n")


def test_save_empty_rows_writes_header_only(tmp_path):
    path = tmp_path / "out.csv"
    save(path, [], FIELDS, sort_by=["key"])
    assert path.read_text() == "key,state,employer,first_seen\n"
    assert load(path) == []


def test_save_sorts_rows_so_output_is_independent_of_input_order(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    rows = [_row("k3", "TX"), _row("k1", "TX"), _row("k2", "CA")]
    save(a, rows, FIELDS, sort_by=["state", "key"])
    save(b, list(reversed(rows)), FIELDS, sort_by=["state", "key"])
    assert a.read_bytes() == b.read_bytes()
    assert [r["key"] for r in load(a)] == ["k2", "k1", "k3"]


def test_save_ignores_extra_keys_and_fills_missing_ones(tmp_path):
    path = tmp_path / "out.csv"
    save(path, [{"key": "a", "state": "TX", "junk": "x"}], FIELDS, sort_by=["key"])
    assert load(path) == [{"key": "a", "state": "TX", "employer": "", "first_seen": ""}]


def test_save_leaves_no_temporary_files_behind(tmp_path):
    path = tmp_path / "out.csv"
    save(path, [_row("a")], FIELDS, sort_by=["key"])
    save(path, [_row("a"), _row("b")], FIELDS, sort_by=["key"])
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]


def test_merge_adds_only_new_keys_and_reports_count():
    existing = [_row("a"), _row("b")]
    new = [_row("b", first_seen="2026-02-02"), _row("c", first_seen="2026-02-02")]
    merged, added = merge(existing, new)
    assert added == 1
    assert [r["key"] for r in merged] == ["a", "b", "c"]


def test_merge_keeps_the_existing_row_when_keys_collide():
    existing = [_row("a", employer="Old", first_seen="2026-01-01")]
    merged, added = merge(existing, [_row("a", employer="New", first_seen="2026-09-09")])
    assert added == 0
    assert merged == existing


def test_merge_collapses_duplicates_within_the_new_batch():
    merged, added = merge([], [_row("a", employer="First"), _row("a", employer="Second")])
    assert added == 1
    assert merged == [_row("a", employer="First")]


def test_merge_does_not_mutate_its_inputs():
    existing = [_row("a")]
    new = [_row("b")]
    merge(existing, new)
    assert existing == [_row("a")]
    assert new == [_row("b")]


def test_merge_supports_a_custom_key_column():
    merged, added = merge(
        [{"id": "1", "v": "x"}], [{"id": "1", "v": "y"}, {"id": "2", "v": "z"}], key="id"
    )
    assert added == 1
    assert merged == [{"id": "1", "v": "x"}, {"id": "2", "v": "z"}]


def test_running_the_same_merge_and_save_twice_is_a_no_op(tmp_path):
    path = tmp_path / "out.csv"
    batch = [_row("b"), _row("a")]
    merged, _ = merge(load(path), batch)
    save(path, merged, FIELDS, sort_by=["key"])
    first = path.read_bytes()
    merged, added = merge(load(path), batch)
    save(path, merged, FIELDS, sort_by=["key"])
    assert added == 0
    assert path.read_bytes() == first
