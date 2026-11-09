"""Flat-file persistence helpers for scraped CAM data."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from typing import Any


def load(path: Path) -> list[dict[str, str]]:
    """Load all rows from a CSV as dicts keyed by the header."""
    if not path.exists():
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return [
            {k: ("" if v is None else str(v)) for k, v in row.items()} for row in csv.DictReader(f)
        ]


def merge(
    existing: list[dict[str, str]],
    new: list[dict[str, str]],
    key: str = "key",
) -> tuple[list[dict[str, str]], int]:
    """Merge rows by key, keeping the first-seen row from existing/new."""
    merged = [dict(r) for r in existing]
    seen: set[Any] = {r.get(key) for r in existing}
    batch_seen: set[Any] = set()
    added = 0
    for row in new:
        k = row.get(key)
        if k in seen or k in batch_seen:
            continue
        batch_seen.add(k)
        merged.append(dict(row))
        seen.add(k)
        added += 1
    return merged, added


def save(
    path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
    sort_by: list[str],
) -> None:
    """Save rows as a deterministic, atomic CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(rows, key=lambda r: tuple(r.get(col, "") for col in sort_by))
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            delete=False,
            dir=str(path.parent),
            prefix=f"{path.name}.",
            suffix=".tmp",
        ) as fd:
            tmp_path = Path(fd.name)
            w = csv.writer(fd, lineterminator="\n")
            w.writerow(fieldnames)
            for row in sorted_rows:
                w.writerow([row.get(f, "") for f in fieldnames])
        os.replace(tmp_path, path)
        tmp_path = None
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
