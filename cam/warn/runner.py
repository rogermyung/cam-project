from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from cam import store
from cam.warn import FIELDNAMES, fetch, parse, record_to_row
from cam.warn.state_urls import STATE_CONFIGS

logger = logging.getLogger(__name__)


class RunFailed(RuntimeError):
    """Raised when a WARN tracking run produces nothing usable."""


def run(
    data_dir: Path,
    *,
    states: list[str] | None = None,
    fetcher: Callable[[str], bytes] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    if today is None:
        today = date.today()

    fetcher = fetcher or fetch

    if states is None:
        selected_states = list(STATE_CONFIGS.keys())
    else:
        selected_states = [s.strip().upper() for s in states]
        unknown = [s for s in selected_states if s not in STATE_CONFIGS]
        if unknown:
            raise ValueError(f"Unknown state codes: {', '.join(unknown)}")

    warn_dir = data_dir / "warn"
    notices_path = warn_dir / "notices.csv"
    status_path = warn_dir / "status.json"

    previous_status: dict[str, Any]
    if status_path.exists():
        previous_status = json.loads(status_path.read_text())
    else:
        previous_status = {}

    rows = store.load(notices_path)
    status: dict[str, Any] = dict(previous_status)

    added_total = 0
    any_success = False

    last_attempt = today.isoformat()
    for state_code in selected_states:
        cfg = STATE_CONFIGS[state_code]
        prev_entry = previous_status.get(state_code, {})
        prev_fetched = int(prev_entry.get("fetched", 0) or 0)
        prev_last_success = prev_entry.get("last_success")

        try:
            content = fetcher(cfg.url)
            records = [r for r in parse(content, cfg) if (r.company or "").strip()]
            fetched = len(records)
            if fetched == 0:
                raise ValueError("parsed 0 records")

            new_rows = [
                record_to_row(record, source_url=cfg.url, first_seen=today) for record in records
            ]
            rows, added = store.merge(rows, new_rows, key="key")
            added_total += added
            degraded = fetched * 2 < prev_fetched
            any_success = True

            status[state_code] = {
                "ok": True,
                "fetched": fetched,
                "added": added,
                "error": None,
                "last_attempt": last_attempt,
                "last_success": last_attempt,
                "degraded": degraded,
            }
            logger.info(
                "WARN[%s] ok fetched=%s added=%s degraded=%s",
                state_code,
                fetched,
                added,
                degraded,
            )
        except Exception as e:
            status[state_code] = {
                "ok": False,
                "fetched": prev_fetched,
                "added": 0,
                "error": str(e),
                "last_attempt": last_attempt,
                "last_success": prev_last_success,
                "degraded": False,
            }
            logger.info(
                "WARN[%s] failed fetched=%s error=%s",
                state_code,
                prev_fetched,
                str(e),
            )

    store.save(
        notices_path,
        rows,
        FIELDNAMES,
        sort_by=["state", "notice_date", "key"],
    )
    warn_dir.mkdir(parents=True, exist_ok=True)
    status_json = json.dumps(status, indent=2, sort_keys=True) + "\n"
    status_path.write_text(status_json)

    total_rows = len(rows)
    if not any_success:
        raise RunFailed("WARN run failed: all selected states failed")
    if total_rows == 0:
        raise RunFailed("WARN run failed: no WARN rows found")

    return {
        "states": status,
        "total": total_rows,
        "added": added_total,
    }
