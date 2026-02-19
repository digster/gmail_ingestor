"""Minimal CLI entry point for manual testing of the Gmail Ingestor."""

from __future__ import annotations

import argparse
import logging
import sys

from gmail_ingestor.config.settings import GmailIngestorSettings
from gmail_ingestor.core.models import FetchProgress
from gmail_ingestor.pipeline.ingestor import EmailIngestor


def setup_logging(level: str) -> None:
    """Configure logging with timestamp and module info."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def on_progress(progress: FetchProgress) -> None:
    """Print progress updates to stdout."""
    print(
        f"[{progress.current_stage}] "
        f"discovered={progress.ids_discovered} "
        f"fetched={progress.messages_fetched} "
        f"converted={progress.messages_converted} "
        f"failed={progress.messages_failed}",
        end="\r",
        flush=True,
    )


def _add_pagination_args(subparser: argparse.ArgumentParser) -> None:
    """Add --limit, --offset, and --batch-size flags to a subparser."""
    subparser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap total messages processed in this stage",
    )
    subparser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip the first N messages",
    )
    subparser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        dest="batch_size",
        help="Override batch size for fetch/convert stages",
    )


def _validate_pagination_args(args: argparse.Namespace) -> None:
    """Reject negative pagination values."""
    if getattr(args, "limit", None) is not None and args.limit < 0:
        print("Error: --limit must be non-negative", file=sys.stderr)
        sys.exit(1)
    if getattr(args, "offset", 0) < 0:
        print("Error: --offset must be non-negative", file=sys.stderr)
        sys.exit(1)
    if getattr(args, "batch_size", None) is not None and args.batch_size <= 0:
        print("Error: --batch-size must be positive", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Gmail Ingestor - Fetch emails and convert to markdown"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # list-labels command
    subparsers.add_parser("list-labels", help="List all Gmail labels")

    # fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch and convert emails")
    fetch_parser.add_argument("--label", "-l", help="Gmail label ID (default: from settings)")
    fetch_parser.add_argument("--query", "-q", help="Gmail search query")
    _add_pagination_args(fetch_parser)

    # status command
    subparsers.add_parser("status", help="Show message counts by status")

    # retry command
    subparsers.add_parser("retry", help="Reset failed messages to pending")

    # discovery-only command
    discover_parser = subparsers.add_parser(
        "discover", help="Only discover message IDs (Stage 1)"
    )
    discover_parser.add_argument("--label", "-l", help="Gmail label ID")
    discover_parser.add_argument("--query", "-q", help="Gmail search query")
    _add_pagination_args(discover_parser)

    # fetch-pending command
    fetch_pending_parser = subparsers.add_parser(
        "fetch-pending", help="Fetch pending messages (Stage 2)"
    )
    _add_pagination_args(fetch_pending_parser)

    # convert-pending command
    convert_pending_parser = subparsers.add_parser(
        "convert-pending", help="Convert fetched messages (Stage 3)"
    )
    _add_pagination_args(convert_pending_parser)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Validate pagination args for commands that have them
    if args.command in ("fetch", "discover", "fetch-pending", "convert-pending"):
        _validate_pagination_args(args)

    settings = GmailIngestorSettings()
    setup_logging(settings.log_level)

    ingestor = EmailIngestor(settings=settings, on_progress=on_progress)

    try:
        if args.command == "list-labels":
            labels = ingestor.list_labels()
            print(f"\nFound {len(labels)} labels:\n")
            for label in sorted(labels, key=lambda x: x["name"]):
                print(f"  {label['id']:40s} {label['name']}")

        elif args.command == "fetch":
            label = getattr(args, "label", None)
            query = getattr(args, "query", None)
            progress = ingestor.run(
                label_id=label,
                query=query,
                limit=args.limit,
                offset=args.offset,
                batch_size=args.batch_size,
            )
            print(f"\n\nComplete: {progress}")

        elif args.command == "status":
            counts = ingestor.get_status()
            print("\nMessage counts by status:")
            for status, count in sorted(counts.items()):
                print(f"  {status}: {count}")

        elif args.command == "retry":
            count = ingestor.retry_failed()
            print(f"\nReset {count} failed messages to pending")

        elif args.command == "discover":
            label = getattr(args, "label", None)
            query = getattr(args, "query", None)
            count = ingestor.run_discovery(
                label_id=label,
                query=query,
                limit=args.limit,
                offset=args.offset,
            )
            print(f"\n\nDiscovered {count} new message IDs")

        elif args.command == "fetch-pending":
            count = ingestor.run_fetch_pending(
                limit=args.limit,
                offset=args.offset,
                batch_size=args.batch_size,
            )
            print(f"\n\nFetched {count} messages")

        elif args.command == "convert-pending":
            count = ingestor.run_convert_pending(
                limit=args.limit,
                offset=args.offset,
                batch_size=args.batch_size,
            )
            print(f"\n\nConverted {count} messages")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        ingestor.close()


if __name__ == "__main__":
    main()
