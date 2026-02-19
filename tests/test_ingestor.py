"""Integration tests for EmailIngestor with mocked dependencies."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from gmail_ingestor.config.settings import GmailIngestorSettings
from gmail_ingestor.core.exceptions import ConversionError
from gmail_ingestor.core.models import (
    ConvertedEmail,
    EmailBody,
    EmailHeader,
    EmailMessage,
    FetchProgress,
    MessageStub,
)
from gmail_ingestor.pipeline.ingestor import EmailIngestor


@pytest.fixture
def tmp_settings(tmp_path: Path) -> GmailIngestorSettings:
    """Settings pointing to temporary directories."""
    return GmailIngestorSettings(
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


def _build_ingestor(
    settings: GmailIngestorSettings,
    gmail_client: MagicMock,
    parser: MagicMock,
    converter: MagicMock,
    tracker: MagicMock,
    raw_store: MagicMock,
    writer: MagicMock,
    on_progress: Any = None,
) -> EmailIngestor:
    """Build an EmailIngestor with all internal components replaced by mocks.

    We patch authenticate/build_gmail_service so _ensure_initialized() doesn't
    attempt real OAuth, then override the lazy-init components directly.
    """
    with (
        patch("gmail_ingestor.pipeline.ingestor.authenticate") as mock_auth,
        patch("gmail_ingestor.pipeline.ingestor.build_gmail_service") as mock_build,
    ):
        mock_auth.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        ingestor = EmailIngestor(settings=settings, on_progress=on_progress)

        # Replace lazy components with mocks
        ingestor._client = gmail_client
        ingestor._parser = parser
        ingestor._converter = converter
        ingestor._tracker = tracker
        ingestor._raw_store = raw_store
        ingestor._writer = writer

    return ingestor


# ---------- run_discovery ----------


class TestRunDiscovery:
    """Tests for EmailIngestor.run_discovery()."""

    def test_inserts_pending_messages_from_paginated_discovery(
        self,
        tmp_settings: GmailIngestorSettings,
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

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total_new = ingestor.run_discovery("INBOX")

        assert total_new == 3
        assert mock_tracker.bulk_insert_pending.call_count == 2
        mock_tracker.bulk_insert_pending.assert_any_call([("msg1", "t1"), ("msg2", "t2")], "INBOX")
        mock_tracker.bulk_insert_pending.assert_any_call([("msg3", "t3")], "INBOX")
        assert ingestor._progress.ids_discovered == 3

    def test_returns_zero_when_no_messages_discovered(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_discovery() returns 0 when no pages are yielded."""
        mock_gmail_client.discover_message_ids.return_value = iter([])

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total_new = ingestor.run_discovery("INBOX")

        assert total_new == 0
        mock_tracker.bulk_insert_pending.assert_not_called()


# ---------- run_fetch_pending ----------


class TestRunFetchPending:
    """Tests for EmailIngestor.run_fetch_pending()."""

    def test_fetches_parses_stores_and_marks_fetched(
        self,
        tmp_settings: GmailIngestorSettings,
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

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingestor.run_fetch_pending()

        assert total == 2
        assert mock_parser.parse.call_count == 2
        assert mock_raw_store.store.call_count == 2
        # Both messages should be marked as fetched
        fetched_calls = [
            c for c in mock_tracker.update_status.call_args_list if c[0][1] == "fetched"
        ]
        assert len(fetched_calls) == 2
        assert ingestor._progress.messages_fetched == 2

    def test_failed_messages_tracked_with_error(
        self,
        tmp_settings: GmailIngestorSettings,
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

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingestor.run_fetch_pending()

        assert total == 0
        # Message should be marked as failed
        failed_calls = [c for c in mock_tracker.update_status.call_args_list if c[0][1] == "failed"]
        assert len(failed_calls) >= 1
        assert "MIME decode error" in str(failed_calls[0])
        assert ingestor._progress.messages_failed >= 1


# ---------- run_convert_pending ----------


class TestRunConvertPending:
    """Tests for EmailIngestor.run_convert_pending()."""

    def test_converts_fetched_to_markdown_and_writes_files(
        self,
        tmp_settings: GmailIngestorSettings,
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

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingestor.run_convert_pending()

        assert total == 1
        mock_converter.convert.assert_called_once()
        mock_writer.write.assert_called_once_with(converted)
        # Should be marked as converted
        converted_calls = [
            c for c in mock_tracker.update_status.call_args_list if c[0][1] == "converted"
        ]
        assert len(converted_calls) == 1
        assert ingestor._progress.messages_converted == 1

    def test_conversion_failure_marks_message_as_failed(
        self,
        tmp_settings: GmailIngestorSettings,
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

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
        )

        total = ingestor.run_convert_pending()

        assert total == 0
        failed_calls = [c for c in mock_tracker.update_status.call_args_list if c[0][1] == "failed"]
        assert len(failed_calls) == 1
        assert "No convertible content" in str(failed_calls[0])
        assert ingestor._progress.messages_failed == 1


# ---------- Progress callback ----------


class TestProgressCallback:
    """Tests for progress callback invocation."""

    def test_progress_callback_is_called_during_discovery(
        self,
        tmp_settings: GmailIngestorSettings,
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

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
            on_progress=capture_progress,
        )

        ingestor.run_discovery("INBOX")

        # At least one progress update should have been fired
        assert len(progress_updates) >= 1
        # The stage should be "discovery"
        assert any(p.current_stage == "discovery" for p in progress_updates)
        # The last update should reflect 1 discovered ID
        discovery_updates = [p for p in progress_updates if p.current_stage == "discovery"]
        assert discovery_updates[-1].ids_discovered == 1

    def test_progress_callback_is_called_during_fetch(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """on_progress callback is invoked during run_fetch_pending()."""
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
        email = EmailMessage(
            message_id="msg1", thread_id="t1", header=header, body=body,
        )
        mock_parser.parse.return_value = email
        mock_raw_store.store.return_value = {}
        mock_tracker.get_message.return_value = None

        ingestor = _build_ingestor(
            tmp_settings,
            mock_gmail_client,
            mock_parser,
            mock_converter,
            mock_tracker,
            mock_raw_store,
            mock_writer,
            on_progress=capture_progress,
        )

        ingestor.run_fetch_pending()

        fetch_updates = [
            p for p in progress_updates if p.current_stage == "fetch"
        ]
        assert len(fetch_updates) >= 1
        assert any(p.messages_fetched >= 1 for p in fetch_updates)


# ---------- Discovery pagination ----------


class TestDiscoveryPagination:
    """Tests for pagination in run_discovery()."""

    def test_discovery_with_limit(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_discovery(limit=15) stops after collecting 15 stubs across pages."""
        page1 = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(10)]
        page2 = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(10, 20)]
        page3 = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(20, 30)]
        mock_gmail_client.discover_message_ids.return_value = iter([page1, page2, page3])
        mock_tracker.bulk_insert_pending.return_value = 0

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        ingestor.run_discovery("INBOX", limit=15)

        # Should have inserted 10 from page1 + 5 from page2
        calls = mock_tracker.bulk_insert_pending.call_args_list
        assert len(calls) == 2
        assert len(calls[0][0][0]) == 10  # full page1
        assert len(calls[1][0][0]) == 5   # first 5 of page2

    def test_discovery_with_offset(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_discovery(offset=3) skips first 3 stubs."""
        page = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(10)]
        mock_gmail_client.discover_message_ids.return_value = iter([page])
        mock_tracker.bulk_insert_pending.return_value = 7

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        total = ingestor.run_discovery("INBOX", offset=3)

        assert total == 7
        calls = mock_tracker.bulk_insert_pending.call_args_list
        assert len(calls) == 1
        inserted_stubs = calls[0][0][0]
        assert len(inserted_stubs) == 7
        # Should start from m3 (skipping m0, m1, m2)
        assert inserted_stubs[0] == ("m3", "t3")

    def test_discovery_with_limit_and_offset(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """Offset=5 limit=10 with 20 available inserts exactly 10 after skip."""
        page1 = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(10)]
        page2 = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(10, 20)]
        mock_gmail_client.discover_message_ids.return_value = iter([page1, page2])
        mock_tracker.bulk_insert_pending.return_value = 0

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        ingestor.run_discovery("INBOX", offset=5, limit=10)

        calls = mock_tracker.bulk_insert_pending.call_args_list
        total_stubs = sum(len(c[0][0]) for c in calls)
        assert total_stubs == 10
        # First batch: m5..m9 (5 stubs from page1 after skipping 5)
        assert len(calls[0][0][0]) == 5
        assert calls[0][0][0][0] == ("m5", "t5")
        # Second batch: m10..m14 (5 stubs from page2)
        assert len(calls[1][0][0]) == 5
        assert calls[1][0][0][0] == ("m10", "t10")


# ---------- Fetch pending pagination ----------


class TestFetchPendingPagination:
    """Tests for pagination in run_fetch_pending()."""

    def test_fetch_pending_with_limit(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_fetch_pending(limit=3) stops after fetching 3 messages."""
        # Return 3 IDs in first batch (which is the limit), then stop
        mock_tracker.get_pending_ids.side_effect = [["m1", "m2", "m3"], []]

        header = EmailHeader(
            subject="Test", sender="s@test.com", to="r@test.com",
            date=datetime(2024, 1, 1),
        )
        body = EmailBody(plain_text="Hello")

        def make_email(raw_msg: dict) -> EmailMessage:
            msg_id = raw_msg["id"]
            return EmailMessage(message_id=msg_id, thread_id="t1", header=header, body=body)

        mock_parser.parse.side_effect = make_email
        mock_gmail_client.fetch_messages_batch.return_value = [
            {"id": "m1"}, {"id": "m2"}, {"id": "m3"},
        ]
        mock_raw_store.store.return_value = {}
        mock_tracker.get_message.return_value = None

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        total = ingestor.run_fetch_pending(limit=3)
        assert total == 3

    def test_fetch_pending_with_offset(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_fetch_pending(offset=5) passes offset to get_pending_ids."""
        mock_tracker.get_pending_ids.return_value = []

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        ingestor.run_fetch_pending(offset=5)

        mock_tracker.get_pending_ids.assert_called_with(
            limit=tmp_settings.batch_size, offset=5,
        )

    def test_fetch_pending_with_custom_batch_size(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run_fetch_pending(batch_size=10) uses overridden batch size."""
        mock_tracker.get_pending_ids.return_value = []

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        ingestor.run_fetch_pending(batch_size=10)

        mock_tracker.get_pending_ids.assert_called_with(limit=10, offset=0)


# ---------- Convert pending pagination ----------


class TestConvertPendingPagination:
    """Tests for pagination in run_convert_pending()."""

    def test_convert_pending_with_limit(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_convert_pending(limit=1) converts only 1 message."""
        mock_tracker.get_fetched_ids.side_effect = [["msg1"], []]

        raw_text_path = tmp_path / "raw" / "msg1.txt"
        raw_text_path.parent.mkdir(parents=True, exist_ok=True)
        raw_text_path.write_text("Hello world", encoding="utf-8")

        mock_tracker.get_message.return_value = {
            "message_id": "msg1",
            "subject": "Test",
            "sender": "s@test.com",
            "date": "2024-06-15T12:00:00",
            "raw_text_path": str(raw_text_path),
            "raw_html_path": "",
            "status": "fetched",
        }

        header = EmailHeader(
            subject="Test", sender="s@test.com", to="", date=datetime(2024, 6, 15, 12, 0, 0),
        )
        converted = ConvertedEmail(
            message_id="msg1",
            markdown="---\nsubject: Test\n---\nHello",
            header=header,
        )
        mock_converter.convert.return_value = converted
        mock_writer.write.return_value = tmp_path / "output" / "test.md"

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        total = ingestor.run_convert_pending(limit=1)
        assert total == 1


# ---------- Full pipeline pagination ----------


class TestRunPipelinePagination:
    """Tests for pagination in the full run() pipeline."""

    def test_run_full_pipeline_limit_caps_discovery(
        self,
        tmp_settings: GmailIngestorSettings,
        mock_gmail_client: MagicMock,
        mock_parser: MagicMock,
        mock_converter: MagicMock,
        mock_tracker: MagicMock,
        mock_raw_store: MagicMock,
        mock_writer: MagicMock,
    ) -> None:
        """run(limit=10) passes limit to discovery, batch_size to fetch/convert."""
        page = [MessageStub(message_id=f"m{i}", thread_id=f"t{i}") for i in range(10)]
        mock_gmail_client.discover_message_ids.return_value = iter([page])
        mock_tracker.bulk_insert_pending.return_value = 10
        mock_tracker.get_pending_ids.return_value = []
        mock_tracker.get_fetched_ids.return_value = []

        ingestor = _build_ingestor(
            tmp_settings, mock_gmail_client, mock_parser, mock_converter,
            mock_tracker, mock_raw_store, mock_writer,
        )

        ingestor.run(label_id="INBOX", limit=10, batch_size=25)

        # Discovery should have been called with limit
        calls = mock_tracker.bulk_insert_pending.call_args_list
        total_stubs = sum(len(c[0][0]) for c in calls)
        assert total_stubs == 10

        # Fetch/convert should use the custom batch size
        mock_tracker.get_pending_ids.assert_called_with(limit=25, offset=0)
        mock_tracker.get_fetched_ids.assert_called_with(limit=25, offset=0)
