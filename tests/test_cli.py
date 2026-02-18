"""Tests for CLI argument parsing of pagination flags."""

from __future__ import annotations

import argparse
import sys
from unittest.mock import patch

import pytest


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Import and run the CLI parser on the given argv."""
    # We need to import main's parser logic. Since cli.py defines main() which
    # calls parse_args internally, we replicate the parser setup by importing
    # the module and invoking parse_args with our argv.
    with patch.object(sys, "argv", ["cli.py", *argv]):
        # Re-import to get a fresh parser
        import importlib

        import scripts.cli as cli_module

        importlib.reload(cli_module)

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        fetch_parser = subparsers.add_parser("fetch")
        fetch_parser.add_argument("--label", "-l")
        fetch_parser.add_argument("--query", "-q")
        cli_module._add_pagination_args(fetch_parser)

        discover_parser = subparsers.add_parser("discover")
        discover_parser.add_argument("--label", "-l")
        discover_parser.add_argument("--query", "-q")
        cli_module._add_pagination_args(discover_parser)

        fetch_pending_parser = subparsers.add_parser("fetch-pending")
        cli_module._add_pagination_args(fetch_pending_parser)

        convert_pending_parser = subparsers.add_parser("convert-pending")
        cli_module._add_pagination_args(convert_pending_parser)

        return parser.parse_args(argv)


class TestFetchPaginationArgs:
    """Test --limit, --offset, --batch-size on 'fetch' subcommand."""

    def test_defaults(self) -> None:
        args = _parse_args(["fetch", "--label", "INBOX"])
        assert args.limit is None
        assert args.offset == 0
        assert args.batch_size is None

    def test_all_flags(self) -> None:
        args = _parse_args(
            ["fetch", "--label", "INBOX", "--limit", "20", "--offset", "50", "--batch-size", "25"]
        )
        assert args.limit == 20
        assert args.offset == 50
        assert args.batch_size == 25


class TestDiscoverPaginationArgs:
    """Test --limit, --offset, --batch-size on 'discover' subcommand."""

    def test_defaults(self) -> None:
        args = _parse_args(["discover", "--label", "INBOX"])
        assert args.limit is None
        assert args.offset == 0
        assert args.batch_size is None

    def test_limit_and_offset(self) -> None:
        args = _parse_args(["discover", "--label", "INBOX", "--limit", "10", "--offset", "5"])
        assert args.limit == 10
        assert args.offset == 5


class TestFetchPendingPaginationArgs:
    """Test --limit, --offset, --batch-size on 'fetch-pending' subcommand."""

    def test_defaults(self) -> None:
        args = _parse_args(["fetch-pending"])
        assert args.limit is None
        assert args.offset == 0
        assert args.batch_size is None

    def test_all_flags(self) -> None:
        args = _parse_args(["fetch-pending", "--limit", "5", "--batch-size", "10"])
        assert args.limit == 5
        assert args.batch_size == 10


class TestConvertPendingPaginationArgs:
    """Test --limit, --offset, --batch-size on 'convert-pending' subcommand."""

    def test_defaults(self) -> None:
        args = _parse_args(["convert-pending"])
        assert args.limit is None
        assert args.offset == 0
        assert args.batch_size is None

    def test_all_flags(self) -> None:
        args = _parse_args(
            ["convert-pending", "--limit", "3", "--offset", "2", "--batch-size", "15"]
        )
        assert args.limit == 3
        assert args.offset == 2
        assert args.batch_size == 15


class TestValidation:
    """Test that _validate_pagination_args rejects negative values."""

    def test_negative_limit_exits(self) -> None:
        import scripts.cli as cli_module

        args = argparse.Namespace(command="fetch", limit=-1, offset=0, batch_size=None)
        with pytest.raises(SystemExit):
            cli_module._validate_pagination_args(args)

    def test_negative_offset_exits(self) -> None:
        import scripts.cli as cli_module

        args = argparse.Namespace(command="fetch", limit=None, offset=-5, batch_size=None)
        with pytest.raises(SystemExit):
            cli_module._validate_pagination_args(args)

    def test_zero_batch_size_exits(self) -> None:
        import scripts.cli as cli_module

        args = argparse.Namespace(command="fetch", limit=None, offset=0, batch_size=0)
        with pytest.raises(SystemExit):
            cli_module._validate_pagination_args(args)

    def test_valid_args_pass(self) -> None:
        import scripts.cli as cli_module

        args = argparse.Namespace(command="fetch", limit=10, offset=5, batch_size=25)
        # Should not raise
        cli_module._validate_pagination_args(args)
