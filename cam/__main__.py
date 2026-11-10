from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any


def _parse_states(states_csv: str | None) -> list[str] | None:
    if states_csv is None:
        return None
    parts = [p.strip().upper() for p in states_csv.split(",") if p.strip()]
    return parts or None


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(prog="cam")
    subs = parser.add_subparsers(dest="cmd")
    warn_p = subs.add_parser("warn", help="Fetch and track WARN layoff notices")
    warn_p.add_argument("--data-dir", default="data")
    warn_p.add_argument("--site-dir", default="site/data")
    warn_p.add_argument("--states", default=None)
    warn_p.add_argument("--no-build", action="store_true")

    if not argv or argv[0] != "warn":
        parser.print_help()
        return 2

    args = parser.parse_args(argv)

    from cam.warn import runner

    selected_states = _parse_states(args.states)
    try:
        report = runner.run(
            Path(args.data_dir),
            states=selected_states,
        )
    except runner.RunFailed as e:
        print(str(e), file=sys.stderr)
        return 1

    states_report: dict[str, Any] = report["states"]
    for state_code in sorted(states_report.keys()):
        entry = states_report[state_code]
        ok = entry.get("ok")
        status_txt = "ok" if ok else "FAILED"
        fetched = entry.get("fetched")
        added = entry.get("added")
        err = entry.get("error")
        if err:
            print(f"{state_code} {status_txt} fetched={fetched} added={added} error={err}")
        else:
            print(f"{state_code} {status_txt} fetched={fetched} added={added}")

    print(f"Totals total={report['total']} added={report['added']}")

    if not args.no_build:
        from cam.warn.build import build

        build(Path(args.data_dir), Path(args.site_dir))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
