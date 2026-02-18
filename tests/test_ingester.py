"""Integration tests for EmailIngester with mocked dependencies."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from gmail_ingester.config.settings import GmailIngesterSettings
from gmail_ingester.core.exceptions import ConversionError
from gmail_ingester.core.models import (
    ConvertedEmail,
    EmailBody,
    EmailHeader,
    EmailMessage,
    FetchProgress,
    MessageStub,
)
from gmail_ingester.pipeline.ingester import EmailIngester


@pytest.fixture
def tmp_settings(tmp_path: Path) -> GmailIngesterSettings:
    """Settings pointing to temporary directories."""
    return GmailIngesterSettings(
        credentials_path=tmp_path / "creds" / "client_secret.json",
        token_path=tmp_path / "creds" / "token.json",
        database_path=tmp_path / "data" / "test.db",
        output_markdown_dir=tmp_path / "output" / "markdown",
        output_raw_dir=tmp_path / "output" / "raw",
        label="INBOX",
        max_results_per_page=100,
        batch_size=50,
    )


@pytest.fixture
def mock_gmail_client() -> MagicMock:
    """Mocked GmailClient."""
    return MagicMock()


@pytest.fixture
def mock_parser() -> MagicMock:
    """Mocked GmailParser."""
    return MagicMock()


@pytest.fixture
def mock_converter() -> MagicMock:
    """Mocked MarkdownConverter."""
    return MagicMock()


@pytest.fixture
def mock_tracker() -> MagicMock:
    """Mocked FetchTracker."""
    tracker = MagicMock()
    tracker.start_run.return_value = 1
    return tracker


@pytest.fixture
def mock_raw_store() -> MagicMock:
    """Mocked RawEmailStore."""
    return MagicMock()


@pytest.fixture
def mock_writer() -> MagicMock:
    """Mocked MarkdownWriter."""
    return MagicMock()


def _build_ingester(
    settings: GmailIngesterSettings,
    gmail_client: MagicMock,
    parser: MagicMock,
    converter: MagicMock,
    tracker: MagicMock,
    raw_store: MagicMock,
    writer: MagicMock,
    on_progress: Any = None,
) -> EmailIngester:
    """Build an EmailIngester with all internal components replaced by mocks.

    We patch authenticate/build_gmail_service so _ensure_initialized() doesn't
    attempt real OAuth, then override the lazy-init components directly.
    """
    with (
        patch("gmail_ingester.pipeline.ingester.authenticate") as mock_auth,
        patch("gmail_ingester.pipeline.ingester.build_gmail_service") as mock_build,
    ):
        mock_auth.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        ingester = EmailIngester(settings=settings, on_progress=on_progress)

        # Replace lazy components with mocks
        ingester._client = gmail_client
        ingester._parser = parser
        ingester._converter = converter
        ingester._tracker = tracker
        ingester._raw_store = raw_store
        ingester._writer = writer

    return ingester


# ---------- run_discovery ----------


class TestRunDiscovery:
    """Tests for EmailIngester.run_discovery()."""

    def test_inserts_pending_messages_from_paginated_discovery(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_discovery() iterates pages from GmailClient and bulk-inserts into tracker."""
        page1 = [
            MessageStub(message_id="msg1", thread_id="t1"),
            MessageStub(message_id="msg2", thread_id="t2"),
        ]
        page2 = [
            MessageStub(message_id="msg3", thread_id="t3"),
        ]
        mock_gmail_client.discover_message_ids.return_value = iter([page1, page2])
        mock_tracker.bulk_insert_pending.side_effect = [2, 1]

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total_new = ingester.run_discovery("INBOX")

        assert total_new == 3
        assert mock_tracker.bulk_insert_pending.call_count == 2
        mock_tracker.bulk_insert_pending.assert_any_call([("msg1", "t1"), ("msg2", "t2")], "INBOX")
        mock_tracker.bulk_insert_pending.assert_any_call([("msg3", "t3")], "INBOX")
        assert ingester._progress.ids_discovered == 3

    def test_returns_zero_when_no_messages_discovered(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_discovery() returns 0 when no pages are yielded."""
        mock_gmail_client.discover_message_ids.return_value = iter([])

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total_new = ingester.run_discovery("INBOX")

        assert total_new == 0
        mock_tracker.bulk_insert_pending.assert_not_called()


# ---------- run_fetch_pending ----------


class TestRunFetchPending:
    """Tests for EmailIngester.run_fetch_pending()."""

    def test_fetches_parses_stores_and_marks_fetched(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_fetch_pending() fetches messages, parses, stores raw, and marks fetched."""
        # Tracker yields one batch of pending IDs then empty
        mock_tracker.get_pending_ids.side_effect = [["msg1", "msg2"], []]

        raw_msg1 = {"id": "msg1", "threadId": "t1", "payload": {}}
        raw_msg2 = {"id": "msg2", "threadId": "t2", "payload": {}}
        mock_gmail_client.fetch_messages_batch.return_value = [raw_msg1, raw_msg2]

        header = EmailHeader(
            subject="Test",
            sender="sender@example.com",
            to="to@example.com",
            date=datetime(2024, 6, 15, 12, 0, 0),
        )
        body = EmailBody(plain_text="Hello world", html=None)
        email1 = EmailMessage(message_id="msg1", thread_id="t1", header=header, body=body)
        email2 = EmailMessage(message_id="msg2", thread_id="t2", header=header, body=body)
        mock_parser.parse.side_effect = [email1, email2]

        mock_raw_store.store.return_value = {"text": Path("/tmp/msg.txt")}

        # get_message returns None for IDs not in fetched set (not needed here
        # since all are fetched, but tracker.get_message may be called for
        # unmatched pending IDs)
        mock_tracker.get_message.return_value = None

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingester.run_fetch_pending()

        assert total == 2
        assert mock_parser.parse.call_count == 2
        assert mock_raw_store.store.call_count == 2
        # Both messages should be marked as fetched
        fetched_calls = [
            c for c in mock_tracker.update_status.call_args_list if c[0][1] == "fetched"
        ]
        assert len(fetched_calls) == 2
        assert ingester._progress.messages_fetched == 2

    def test_failed_messages_tracked_with_error(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_fetch_pending() marks messages as failed when parsing raises."""
        mock_tracker.get_pending_ids.side_effect = [["msg1"], []]

        raw_msg1 = {"id": "msg1", "threadId": "t1", "payload": {}}
        mock_gmail_client.fetch_messages_batch.return_value = [raw_msg1]

        mock_parser.parse.side_effect = Exception("MIME decode error")
        mock_tracker.get_message.return_value = None

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingester.run_fetch_pending()

        assert total == 0
        # Message should be marked as failed
        failed_calls = [c for c in mock_tracker.update_status.call_args_list if c[0][1] == "failed"]
        assert len(failed_calls) >= 1
        assert "MIME decode error" in str(failed_calls[0])
        assert ingester._progress.messages_failed >= 1


# ---------- run_convert_pending ----------


class TestRunConvertPending:
    """Tests for EmailIngester.run_convert_pending()."""

    def test_converts_fetched_to_markdown_and_writes_files(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_convert_pending() reads raw, converts, writes markdown, and marks converted."""
        # Tracker returns one batch of fetched IDs, then empty
        mock_tracker.get_fetched_ids.side_effect = [["msg1"], []]

        raw_text_path = tmp_path / "raw" / "msg1.txt"
        raw_text_path.parent.mkdir(parents=True, exist_ok=True)
        raw_text_path.write_text("Hello world", encoding="utf-8")

        mock_tracker.get_message.return_value = {
            "message_id": "msg1",
            "subject": "Test Subject",
            "sender": "sender@example.com",
            "date": "2024-06-15T12:00:00",
            "raw_text_path": str(raw_text_path),
            "raw_html_path": "",
            "status": "fetched",
        }

        header = EmailHeader(
            subject="Test Subject",
            sender="sender@example.com",
            to="",
            date=datetime(2024, 6, 15, 12, 0, 0),
        )
        converted = ConvertedEmail(
            message_id="msg1",
            markdown="---\nsubject: Test\n---\nHello world",
            header=header,
        )
        mock_converter.convert.return_value = converted
        mock_writer.write.return_value = tmp_path / "output" / "2024-06-15_test_msg1.md"

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingester.run_convert_pending()

        assert total == 1
        mock_converter.convert.assert_called_once()
        mock_writer.write.assert_called_once_with(converted)
        # Should be marked as converted
        converted_calls = [
            c for c in mock_tracker.update_status.call_args_list if c[0][1] == "converted"
        ]
        assert len(converted_calls) == 1
        assert ingester._progress.messages_converted == 1

    def test_conversion_failure_marks_message_as_failed(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_convert_pending() marks messages as failed when conversion raises."""
        mock_tracker.get_fetched_ids.side_effect = [["msg1"], []]

        raw_text_path = tmp_path / "raw" / "msg1.txt"
        raw_text_path.parent.mkdir(parents=True, exist_ok=True)
        raw_text_path.write_text("Hello world", encoding="utf-8")

        mock_tracker.get_message.return_value = {
            "message_id": "msg1",
            "subject": "Test Subject",
            "sender": "sender@example.com",
            "date": "2024-06-15T12:00:00",
            "raw_text_path": str(raw_text_path),
            "raw_html_path": "",
            "status": "fetched",
        }

        mock_converter.convert.side_effect = ConversionError("No convertible content")

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingester.run_convert_pending()

        assert total == 0
        failed_calls = [c for c in mock_tracker.update_status.call_args_list if c[0][1] == "failed"]
        assert len(failed_calls) == 1
        assert "No convertible content" in str(failed_calls[0])
        assert ingester._progress.messages_failed == 1


# ---------- Progress callback ----------


class TestProgressCallback:
    """Tests for progress callback invocation."""

    def test_progress_callback_is_called_during_discovery(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """on_progress callback is invoked during run_discovery() with updated counts."""
        progress_updates: list[FetchProgress] = []

        def capture_progress(progress: FetchProgress) -> None:
            # Snapshot the current state (FetchProgress is mutable)
            progress_updates.append(
                FetchProgress(
                    total_estimated=progress.total_estimated,
                    ids_discovered=progress.ids_discovered,
                    messages_fetched=progress.messages_fetched,
                    messages_converted=progress.messages_converted,
                    messages_failed=progress.messages_failed,
                    current_stage=progress.current_stage,
                )
            )

        page1 = [MessageStub(message_id="msg1", thread_id="t1")]
        mock_gmail_client.discover_message_ids.return_value = iter([page1])
        mock_tracker.bulk_insert_pending.return_value = 1

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
            on_progress=capture_progress,
        )

        ingester.run_discovery("INBOX")

        # At least one progress update should have been fired
        assert len(progress_updates) >= 1
        # The stage should be "discovery"
        assert any(p.current_stage == "discovery" for p in progress_updates)
        # The last update should reflect 1 discovered ID
        discovery_updates = [p for p in progress_updates if p.current_stage == "discovery"]
        assert discovery_updates[-1].ids_discovered == 1

    def test_progress_callback_is_called_during_fetch(
        self,
        tmp_settings: GmailIngesterSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """on_progress callback is invoked during run_fetch_pending() for each message."""
        progress_updates: list[FetchProgress] = []

        def capture_progress(progress: FetchProgress) -> None:
            progress_updates.append(
                FetchProgress(
                    total_estimated=progress.total_estimated,
                    ids_discovered=progress.ids_discovered,
                    messages_fetched=progress.messages_fetched,
                    messages_converted=progress.messages_converted,
                    messages_failed=progress.messages_failed,
                    current_stage=progress.current_stage,
                )
            )

        mock_tracker.get_pending_ids.side_effect = [["msg1"], []]
        raw_msg = {"id": "msg1", "threadId": "t1", "payload": {}}
        mock_gmail_client.fetch_messages_batch.return_value = [raw_msg]

        header = EmailHeader(
            subject="Test",
            sender="s@example.com",
            to="r@example.com",
            date=datetime(2024, 1, 1),
        )
        body = EmailBody(plain_text="Hello")
        email = EmailMessage(message_id="msg1", thread_id="t1", header=header, body=body)
        mock_parser.parse.return_value = email
        mock_raw_store.store.return_value = {}
        mock_tracker.get_message.return_value = None

        ingester = _build_ingester(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
            on_progress=capture_progress,
        )

        ingester.run_fetch_pending()

        # Should have progress updates during fetch stage
        fetch_updates = [p for p in progress_updates if p.current_stage == "fetch"]
        assert len(fetch_updates) >= 1
        # At least one should show a fetched message
        assert any(p.messages_fetched >= 1 for p in fetch_updates)
