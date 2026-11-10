from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from cam import store

from . import (
    BULK_URL,
    FIELDNAMES,
    MIN_COMPLAINTS,
    WINDOW_MONTHS,
    aggregate,
    stream_to_tempfile,
    to_rows,
    window_months,
)


class RunFailed(RuntimeError):
    pass


def _status_base(
    *,
    ok: bool,
    error: str | None,
    today: date,
    months: list[str],
    min_complaints: int,
    url: str,
    degraded: bool,
    previous_status: dict[str, Any],
    carry_over_keys: tuple[str, ...],
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "ok": ok,
        "error": error,
        "last_attempt": today.isoformat(),
        "last_success": previous_status.get("last_success"),
        "rows_read": previous_status.get("rows_read", 0) or 0,
        "complaints": previous_status.get("complaints", 0) or 0,
        "companies": previous_status.get("companies", 0) or 0,
        "rows": previous_status.get("rows", 0) or 0,
        "window_start": months[0],
        "window_end": months[-1],
        "min_complaints": min_complaints,
        "source_url": url,
        "degraded": degraded,
    }

    # When carrying over, keep explicit None/0 when missing.
    for k in carry_over_keys:
        if k not in previous_status:
            if k == "last_success":
                status[k] = None
            else:
                status[k] = 0
    return status


def run(
    data_dir: Path,
    *,
    url: str = BULK_URL,
    downloader: Callable[[str], Path] | None = None,
    today: date | None = None,
    min_complaints: int = MIN_COMPLAINTS,
    window: int = WINDOW_MONTHS,
) -> dict[str, Any]:
    if today is None:
        today = date.today()

    months = window_months(today, window)

    cfpb_dir = data_dir / "cfpb"
    monthly_path = cfpb_dir / "monthly.csv"
    status_path = cfpb_dir / "status.json"
    prev_exists = status_path.exists()
    previous_status: dict[str, Any]
    if prev_exists:
        previous_status = json.loads(status_path.read_text(encoding="utf-8"))
    else:
        previous_status = {}

    zip_path: Path | None = None
    downloader_used_default = downloader is None
    try:
        if downloader is None:
            zip_path = stream_to_tempfile(url)
        else:
            zip_path = downloader(url)

        counts, rows_read = aggregate(zip_path, months=months)
        rows = to_rows(counts, min_complaints=min_complaints)
        if not rows:
            raise ValueError(
                f"No CFPB complaint rows after filtering (min_complaints={min_complaints})"
            )

        complaints = sum(int(r["complaints"]) for r in rows)
        companies = len({r["company"] for r in rows})
        degraded = prev_exists and companies * 2 < (int(previous_status.get("companies", 0)) or 0)

        store.save(
            monthly_path,
            rows,
            FIELDNAMES,
            sort_by=["company", "month", "product"],
        )

        status: dict[str, Any] = {
            "ok": True,
            "error": None,
            "last_attempt": today.isoformat(),
            "last_success": today.isoformat(),
            "rows_read": rows_read,
            "complaints": complaints,
            "companies": companies,
            "rows": len(rows),
            "window_start": months[0],
            "window_end": months[-1],
            "min_complaints": min_complaints,
            "source_url": url,
            "degraded": bool(degraded),
        }

        status_json = json.dumps(status, indent=2, sort_keys=True) + "\n"
        cfpb_dir.mkdir(parents=True, exist_ok=True)
        status_path.write_text(status_json, encoding="utf-8")
        return status

    except Exception as e:
        # Do not touch an existing monthly.csv on failure.
        carry = ("last_success", "rows_read", "complaints", "companies", "rows")
        status_fail = _status_base(
            ok=False,
            error=str(e),
            today=today,
            months=months,
            min_complaints=min_complaints,
            url=url,
            degraded=False,
            previous_status=previous_status,
            carry_over_keys=carry,
        )
        status_fail["last_success"] = previous_status.get("last_success") if prev_exists else None
        status_fail_json = json.dumps(status_fail, indent=2, sort_keys=True) + "\n"
        cfpb_dir.mkdir(parents=True, exist_ok=True)
        status_path.write_text(status_fail_json, encoding="utf-8")
        raise RunFailed(f"CFPB run failed: {e}") from e
    finally:
        if downloader_used_default and zip_path is not None:
            zip_path.unlink(missing_ok=True)


__all__ = ["RunFailed", "run"]
