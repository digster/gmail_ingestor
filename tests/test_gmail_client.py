"""Tests for GmailClient with a mocked Gmail API service."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from gmail_ingestor.core.exceptions import GmailIngestorError, RateLimitError
from gmail_ingestor.core.gmail_client import GmailClient
from gmail_ingestor.core.models import MessageStub


@pytest.fixture
def mock_service() -> MagicMock:
    """Create a fully-mocked Gmail API Resource."""
    return MagicMock()


@pytest.fixture
def client(mock_service: MagicMock) -> GmailClient:
    """Create a GmailClient wrapping the mocked service."""
    return GmailClient(mock_service, user_id="me")


# ---------- list_labels ----------


class TestListLabels:
    """Tests for GmailClient.list_labels()."""

    def test_returns_formatted_label_list(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """list_labels() returns a list of dicts with 'id' and 'name' keys."""
        mock_service.users().labels().list().execute.return_value = {
            "labels": [
                {"id": "INBOX", "name": "INBOX", "type": "system"},
                {"id": "Label_1", "name": "Work", "type": "user"},
                {"id": "SENT", "name": "SENT", "type": "system"},
            ]
        }

        result = client.list_labels()

        assert result == [
            {"id": "INBOX", "name": "INBOX"},
            {"id": "Label_1", "name": "Work"},
            {"id": "SENT", "name": "SENT"},
        ]

    def test_returns_empty_list_when_no_labels(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """list_labels() returns [] when the API response has no labels."""
        mock_service.users().labels().list().execute.return_value = {"labels": []}

        result = client.list_labels()

        assert result == []

    def test_raises_gmail_error_on_api_failure(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """list_labels() wraps API exceptions in GmailIngestorError."""
        mock_service.users().labels().list().execute.side_effect = Exception("API unavailable")

        with pytest.raises(GmailIngestorError, match="Failed to list labels"):
            client.list_labels()


# ---------- discover_message_ids ----------


class TestDiscoverMessageIds:
    """Tests for GmailClient.discover_message_ids()."""

    def test_yields_pages_of_message_stubs(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() yields a list of MessageStub per page."""
        mock_service.users().messages().list().execute.return_value = {
            "messages": [
                {"id": "msg1", "threadId": "t1"},
                {"id": "msg2", "threadId": "t2"},
            ]
        }

        pages = list(client.discover_message_ids("INBOX"))

        assert len(pages) == 1
        assert pages[0] == [
            MessageStub(message_id="msg1", thread_id="t1"),
            MessageStub(message_id="msg2", thread_id="t2"),
        ]

    def test_handles_pagination_with_next_page_token(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() follows nextPageToken across multiple pages."""
        page1_response = {
            "messages": [{"id": "msg1", "threadId": "t1"}],
            "nextPageToken": "token_page2",
        }
        page2_response = {
            "messages": [{"id": "msg2", "threadId": "t2"}],
            # No nextPageToken => stop
        }

        # Each call to .list(**kwargs).execute() returns the next page
        mock_list = mock_service.users().messages().list
        mock_list.return_value.execute.side_effect = [page1_response, page2_response]

        pages = list(client.discover_message_ids("INBOX"))

        assert len(pages) == 2
        assert pages[0] == [MessageStub(message_id="msg1", thread_id="t1")]
        assert pages[1] == [MessageStub(message_id="msg2", thread_id="t2")]

    def test_handles_empty_results(self, client: GmailClient, mock_service: MagicMock) -> None:
        """discover_message_ids() yields nothing when messages list is empty."""
        mock_service.users().messages().list().execute.return_value = {"messages": []}

        pages = list(client.discover_message_ids("INBOX"))

        assert pages == []

    def test_handles_no_messages_key(self, client: GmailClient, mock_service: MagicMock) -> None:
        """discover_message_ids() yields nothing when 'messages' key is absent."""
        mock_service.users().messages().list().execute.return_value = {
            "resultSizeEstimate": 0,
        }

        pages = list(client.discover_message_ids("INBOX"))

        assert pages == []

    def test_raises_rate_limit_error_on_429(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() raises RateLimitError on 429/rateLimitExceeded."""
        mock_service.users().messages().list().execute.side_effect = Exception(
            "HttpError 429: rateLimitExceeded"
        )

        with pytest.raises(RateLimitError, match="Rate limited during discovery"):
            list(client.discover_message_ids("INBOX"))

    def test_raises_gmail_error_on_generic_api_failure(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() wraps non-429 errors in GmailIngestorError."""
        mock_service.users().messages().list().execute.side_effect = Exception("Server error 500")

        with pytest.raises(GmailIngestorError, match="Failed to list messages"):
            list(client.discover_message_ids("INBOX"))

    def test_passes_query_parameter(self, client: GmailClient, mock_service: MagicMock) -> None:
        """discover_message_ids() passes the query parameter to the API."""
        mock_service.users().messages().list().execute.return_value = {"messages": []}

        list(client.discover_message_ids("INBOX", query="from:test@example.com"))

        mock_service.users().messages().list.assert_called_with(
            userId="me",
            labelIds=["INBOX"],
            maxResults=100,
            q="from:test@example.com",
        )


# ---------- fetch_messages_batch ----------


class TestFetchMessagesBatch:
    """Tests for GmailClient.fetch_messages_batch()."""

    def test_returns_message_dicts(self, client: GmailClient, mock_service: MagicMock) -> None:
        """fetch_messages_batch() returns list of raw message dicts on success."""
        msg1 = {"id": "msg1", "threadId": "t1", "payload": {}}
        msg2 = {"id": "msg2", "threadId": "t2", "payload": {}}

        def fake_new_batch(callback: Any) -> MagicMock:
            batch = MagicMock()

            def fake_execute() -> None:
                # Simulate successful callbacks for each message
                callback("msg1", msg1, None)
                callback("msg2", msg2, None)

            batch.execute.side_effect = fake_execute
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        result = client.fetch_messages_batch(["msg1", "msg2"])

        assert result == [msg1, msg2]

    def test_handles_batch_callback_errors(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() logs warning for non-rate-limit callback errors."""
        msg1 = {"id": "msg1", "threadId": "t1", "payload": {}}

        def fake_new_batch(callback: Any) -> MagicMock:
            batch = MagicMock()

            def fake_execute() -> None:
                callback("msg1", msg1, None)
                callback("msg2", None, Exception("Not found"))

            batch.execute.side_effect = fake_execute
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        # Non-rate-limit errors are logged but don't raise; partial results returned
        result = client.fetch_messages_batch(["msg1", "msg2"])

        assert result == [msg1]

    def test_raises_rate_limit_error_on_429_in_callback(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() raises RateLimitError when callbacks report 429."""

        def fake_new_batch(callback: Any) -> MagicMock:
            batch = MagicMock()

            def fake_execute() -> None:
                callback("msg1", None, Exception("429 rateLimitExceeded"))

            batch.execute.side_effect = fake_execute
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        with pytest.raises(RateLimitError, match="Rate limited during batch fetch"):
            client.fetch_messages_batch(["msg1"])

    def test_raises_gmail_error_on_batch_execute_failure(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() raises GmailIngestorError when batch.execute() throws."""

        def fake_new_batch(callback: Any) -> MagicMock:
            batch = MagicMock()
            batch.execute.side_effect = Exception("Network timeout")
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        with pytest.raises(GmailIngestorError, match="Batch request failed"):
            client.fetch_messages_batch(["msg1"])

    def test_returns_empty_list_for_empty_input(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() returns [] when no message IDs are given."""

        def fake_new_batch(callback: Any) -> MagicMock:
            batch = MagicMock()
            batch.execute.return_value = None
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        result = client.fetch_messages_batch([])

        assert result == []
