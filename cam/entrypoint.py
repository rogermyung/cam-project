"""
CAM pipeline entrypoint — CLI for running the ingestion, scoring, and export
steps independently.

Usage (direct Python)::

    python -m cam.entrypoint ingest --source all --since 2025-01-01
    python -m cam.entrypoint score  --date 2025-01-15
    python -m cam.entrypoint export --output-dir /out --digest

Usage (Docker)::

    docker run cam ingest --source osha --since 2025-01-01
    docker run cam score  --date today
    docker run cam export --output-dir /out --digest

Steps are designed to be run independently so they can be scheduled at
different frequencies:

    02:00  ingest (all regulatory sources)
    04:00  score  (reads signals written by ingest)
    04:30  export (reads alert_scores written by score)

Each step exits 0 on success and non-zero on failure so schedulers and CI
pipelines can detect and report failures.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("cam.entrypoint")

_ALL_SOURCES = ["osha", "epa", "cfpb", "warn", "edgar"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date_arg(value: str) -> date:
    """Parse a YYYY-MM-DD string or the literal 'today'."""
    if value.lower() == "today":
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD or 'today'.")


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


def _cmd_ingest(args: argparse.Namespace) -> int:
    """Run one or more ingestion modules and return an exit code."""
    from cam.config import get_settings

    cfg = get_settings()
    since: date = args.since or (date.today() - timedelta(days=cfg.ingest_default_since_days))
    # Expand "all" and deduplicate, preserving any explicitly named sources.
    raw: list[str] = args.source
    sources: list[str] = list(
        dict.fromkeys(s for token in raw for s in (_ALL_SOURCES if token == "all" else [token]))
    )

    logger.info("Ingest starting — sources=%s since=%s", sources, since)

    failures: list[str] = []

    for source in sources:
        try:
            _ingest_source(source, since, args)
        except Exception as exc:
            logger.error("Source '%s' failed: %s", source, exc, exc_info=True)
            failures.append(source)

    if failures:
        logger.error("Ingestion finished with failures: %s", failures)
        return 1

    logger.info("Ingestion complete — all sources succeeded.")
    return 0


def _ingest_source(source: str, since: date, args: argparse.Namespace) -> None:
    """Dispatch a single source ingestion and log the result."""
    from cam.db.session import get_session

    if source == "osha":
        import httpx

        from cam.ingestion.osha import download_bulk_data, ingest_from_csv

        total_ingested = 0
        # Start one year before since.year so that cross-year windows (e.g.
        # since=2025-12-01 run in 2026) pick up prior-year data, and so that
        # when the current-year CSV is not yet published we still attempt the
        # prior year.  ingest_from_csv filters by since_date so no out-of-window
        # rows are admitted.
        start_year = since.year - 1
        for year in range(start_year, date.today().year + 1):
            try:
                csv_path = download_bulk_data(year)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.warning("OSHA bulk CSV for %d not yet published (404), skipping", year)
                    continue
                raise
            with get_session() as db:
                result = ingest_from_csv(csv_path, since_date=since, db=db)
            total_ingested += result.ingested
        logger.info("osha: %s records ingested", total_ingested)

    elif source == "epa":
        from cam.ingestion.epa import ingest_echo_violations

        with get_session() as db:
            count = ingest_echo_violations(since_date=since, db=db)
        logger.info("epa: %s records ingested", count)

    elif source == "cfpb":
        from cam.ingestion.cfpb import ingest_complaints

        with get_session() as db:
            count = ingest_complaints(since_date=since, db=db)
        logger.info("cfpb: %s records ingested", count)

    elif source == "warn":
        from cam.ingestion.warn import ingest_all_states

        with get_session() as db:
            summary = ingest_all_states(since_date=since, db=db)
        logger.info("warn: %s", summary)

    elif source == "edgar":
        from cam.ingestion.edgar import ingest_all_10k

        with get_session() as db:
            count = ingest_all_10k(since_date=since, entity_ids=None, db=db)
        logger.info("edgar: %s filings ingested", count)

    else:
        raise ValueError(f"Unknown source: {source!r}")


# ---------------------------------------------------------------------------
# score
# ---------------------------------------------------------------------------


def _cmd_score(args: argparse.Namespace) -> int:
    """Run daily scoring for all entities and return an exit code."""
    from cam.alerts.scorer import generate_alert, get_prior_score, run_daily_scoring
    from cam.db.session import get_session

    score_date: date = args.date or date.today()
    logger.info("Scoring starting — date=%s", score_date)

    alerts_fired = 0
    try:
        with get_session() as db:
            scores = run_daily_scoring(score_date=score_date, db=db)
            logger.info("Scored %d entities", len(scores))

            for score in scores:
                prior = get_prior_score(score.entity_id, before_date=score_date, db=db)
                alert = generate_alert(score.entity_id, score, prior, db=db)
                if alert:
                    alerts_fired += 1
                    logger.info(
                        "ALERT [%s] entity=%s score=%.3f",
                        (alert.alert_level or "").upper(),
                        score.entity_id,
                        score.composite_score,
                    )
            db.commit()
    except Exception as exc:
        logger.error("Scoring failed: %s", exc, exc_info=True)
        return 1

    logger.info("Scoring complete — %d alerts fired.", alerts_fired)
    return 0


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


def _cmd_export(args: argparse.Namespace) -> int:
    """Export the static site (and optionally digest) and return an exit code."""
    from cam.db.session import get_session
    from cam.output import export_digest, export_static_site

    output_dir: str = args.output_dir
    logger.info("Export starting — output_dir=%s", output_dir)

    try:
        with get_session() as db:
            result = export_static_site(output_dir, db=db)
        logger.info(
            "Static site exported: %d entities, %d alerts, %d files written.",
            result["entities"],
            result["alerts"],
            result["files_written"],
        )

        if args.digest:
            since: date = args.digest_since or (date.today() - timedelta(days=7))
            with get_session() as db:
                body = export_digest(since, db=db)

            from pathlib import Path

            digest_path = Path(output_dir) / "digest.txt"
            digest_path.write_text(body, encoding="utf-8")
            logger.info("Digest written to %s (since=%s)", digest_path, since)

    except Exception as exc:
        logger.error("Export failed: %s", exc, exc_info=True)
        return 1

    logger.info("Export complete.")
    return 0


# ---------------------------------------------------------------------------
# CLI definition
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cam",
        description="Corporate Accountability Monitor — pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s ingest --source all --since 2025-01-01
  %(prog)s ingest --source osha --since 2025-01-01
  %(prog)s score  --date today
  %(prog)s export --output-dir /srv/cam-site --digest
        """,
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    # ---- ingest ----
    p_ingest = sub.add_parser(
        "ingest",
        help="Fetch regulatory data from one or more sources",
        description=(
            "Ingest regulatory data into the events and signals tables. "
            "Each source is independent — a failure in one source does not "
            "stop the others (exit code is non-zero if any source failed)."
        ),
    )
    p_ingest.add_argument(
        "--source",
        nargs="+",
        choices=[*_ALL_SOURCES, "all"],
        default=["all"],
        metavar="SOURCE",
        help=(
            "Data source(s) to ingest. "
            f"Choices: {' '.join(_ALL_SOURCES)} all. "
            f"Default: all ({' + '.join(_ALL_SOURCES)})"
        ),
    )
    p_ingest.add_argument(
        "--since",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="Ingest records on or after this date (default: configured ingest_default_since_days ago)",
    )

    # ---- score ----
    p_score = sub.add_parser(
        "score",
        help="Compute composite alert scores for all entities",
        description=(
            "Reads signals from the signals table, computes weighted composite "
            "scores, writes to alert_scores, and fires alerts when an entity "
            "crosses a threshold for the first time."
        ),
    )
    p_score.add_argument(
        "--date",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="Score date (default: today)",
    )

    # ---- export ----
    p_export = sub.add_parser(
        "export",
        help="Export the static site dashboard and optional weekly digest",
        description=(
            "Reads alert_scores, entities, and signals and writes a self-contained "
            "directory of JSON + JS data files and HTML dashboard pages. "
            "Output can be hosted on S3, GitHub Pages, or opened directly "
            "from the filesystem via file:// URIs."
        ),
    )
    p_export.add_argument(
        "--output-dir",
        required=True,
        metavar="PATH",
        help="Destination directory for static site files (created if absent)",
    )
    p_export.add_argument(
        "--digest",
        action="store_true",
        help="Also write digest.txt (plaintext weekly email body) to output-dir",
    )
    p_export.add_argument(
        "--digest-since",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="Digest covers alerts on or after this date (default: 7 days ago)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate command handler."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    logger.info(
        "CAM entrypoint — command=%s  started=%s",
        args.command,
        datetime.now(UTC).isoformat(timespec="seconds"),
    )

    if args.command == "ingest":
        return _cmd_ingest(args)
    if args.command == "score":
        return _cmd_score(args)
    if args.command == "export":
        return _cmd_export(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
