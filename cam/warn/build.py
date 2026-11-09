from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from cam.store import load
from cam.warn import FIELDNAMES


def _parse_optional_date(value: str) -> str | None:
    raw = (value or "").strip()
    return None if raw == "" else raw


def _parse_optional_int(value: str) -> int | None:
    raw = (value or "").strip()
    return None if raw == "" else int(raw)


def build(data_dir: Path, site_dir: Path, *, now: datetime | None = None) -> dict:
    notices_path = data_dir / "warn" / "notices.csv"
    raw_rows = load(notices_path)
    if not raw_rows:
        raise ValueError("No WARN notices rows found")

    status_path = data_dir / "warn" / "status.json"
    if status_path.exists():
        status: dict = json.loads(status_path.read_text(encoding="utf-8"))
    else:
        status = {}

    if now is None:
        now = datetime.now(UTC)

    notices: list[dict] = []
    for row in raw_rows:
        notice_date = _parse_optional_date(row.get("notice_date", ""))
        employees = _parse_optional_int(row.get("employees", ""))
        obj: dict = {}
        for field in FIELDNAMES:
            if field == "notice_date":
                obj[field] = notice_date
            elif field == "employees":
                obj[field] = employees
            else:
                obj[field] = row.get(field, "")
        notices.append(obj)

    def _notice_sort_key(n: dict) -> tuple[int, int, str]:
        d = n["notice_date"]
        date_int = int(d.replace("-", "")) if d is not None else 0
        return (0 if d is not None else 1, -date_int, n["key"])

    notices.sort(key=_notice_sort_key)

    total_notices = len(notices)
    total_employees = sum((n["employees"] or 0) for n in notices)
    dated_notice_dates = [n["notice_date"] for n in notices if n["notice_date"] is not None]
    latest_notice_date = max(dated_notice_dates) if dated_notice_dates else None

    by_state_counts: dict[str, dict[str, int]] = {}
    by_month_counts: dict[str, dict[str, int]] = {}
    for n in notices:
        state = n["state"]
        by_state_counts.setdefault(state, {"notices": 0, "employees": 0})
        by_state_counts[state]["notices"] += 1
        by_state_counts[state]["employees"] += n["employees"] or 0

        if n["notice_date"] is not None:
            month = n["notice_date"][:7]
            by_month_counts.setdefault(month, {"notices": 0, "employees": 0})
            by_month_counts[month]["notices"] += 1
            by_month_counts[month]["employees"] += n["employees"] or 0

    summary_by_state = [
        {
            "state": s,
            "notices": by_state_counts[s]["notices"],
            "employees": by_state_counts[s]["employees"],
        }
        for s in sorted(by_state_counts)
    ]

    summary_by_month = [
        {
            "month": m,
            "notices": by_month_counts[m]["notices"],
            "employees": by_month_counts[m]["employees"],
        }
        for m in sorted(by_month_counts)
    ]

    largest = [n for n in notices if n["employees"] is not None]
    largest.sort(key=lambda n: (-n["employees"], n["key"]))
    largest = largest[:20]

    repeat_groups: dict[tuple[str, str], dict] = {}
    for n in notices:
        state = n["state"]
        employer_norm = (n["employer"] or "").strip().casefold()
        employer_display = (n["employer"] or "").strip()
        gkey = (state, employer_norm)
        group = repeat_groups.get(gkey)
        if group is None:
            repeat_groups[gkey] = {
                "state": state,
                "employer": employer_display,
                "notices": 0,
                "employees": 0,
            }
            group = repeat_groups[gkey]
        group["notices"] += 1
        group["employees"] += n["employees"] or 0

    repeat_employers = [g for g in repeat_groups.values() if g["notices"] >= 2]
    repeat_employers.sort(key=lambda g: (-g["notices"], -g["employees"], g["state"], g["employer"]))

    summary = {
        "by_state": summary_by_state,
        "by_month": summary_by_month,
        "largest": largest,
        "repeat_employers": repeat_employers,
    }

    meta = {
        "generated_at": now.isoformat(),
        "total_notices": total_notices,
        "total_employees": total_employees,
        "latest_notice_date": latest_notice_date,
        "states": status,
    }

    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "notices.json").write_text(
        json.dumps(notices, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (site_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (site_dir / "meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    return meta
