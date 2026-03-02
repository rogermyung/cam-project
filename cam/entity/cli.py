"""
CLI for managing the manual entity review queue.

Usage:
    python -m cam.entity.cli list
    python -m cam.entity.cli accept <raw_name> <entity_id>
    python -m cam.entity.cli reject <raw_name>
"""

import argparse
import sys

from cam.entity.resolver import get_review_queue_from_db


def cmd_list(_args) -> None:
    from cam.db.session import get_session_factory

    Session = get_session_factory()
    db = Session()
    try:
        queue = get_review_queue_from_db(db)
    finally:
        db.close()

    if not queue:
        print("Review queue is empty.")
        return
    print(f"{'#':<4} {'Raw Name':<50} {'Source':<12} {'Conf':>6}  {'Best Match'}")
    print("-" * 100)
    for i, item in enumerate(queue):
        print(
            f"{i:<4} {item.raw_name:<50} {item.source:<12} "
            f"{item.confidence:>6.2f}  {item.best_match_name or '—'}"
        )


def cmd_accept(args) -> None:
    """Accept a review item: create an alias mapping raw_name to entity_id."""
    import uuid

    from cam.db.session import get_session_factory
    from cam.entity.resolver import add_alias

    Session = get_session_factory()
    db = Session()
    try:
        add_alias(
            entity_id=uuid.UUID(args.entity_id),
            raw_name=args.raw_name,
            source=args.source,
            confidence=1.0,
            db=db,
        )
        db.commit()
        print(f"Alias added: {args.raw_name!r} -> {args.entity_id}")
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cam-entity",
        description="Manage the entity resolution manual review queue.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List items in the review queue")

    accept_p = sub.add_parser("accept", help="Accept a match and persist alias")
    accept_p.add_argument("raw_name", help="The raw company name to alias")
    accept_p.add_argument("entity_id", help="UUID of the canonical entity")
    accept_p.add_argument("--source", default="manual", help="Source tag for the alias")

    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "list":
        cmd_list(args)
    elif args.command == "accept":
        cmd_accept(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
