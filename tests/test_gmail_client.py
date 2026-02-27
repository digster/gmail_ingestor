"""Tests for GmailClient with a mocked Gmail API service."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from gmail_ingestor.core.exceptions import GmailIngestorError, RateLimitError
from gmail_ingestor.core.gmail_client import GmailClient, _is_rate_limit_error
from gmail_ingestor.core.models import MessageStub


@pytest.fixture
def mock_service() -> MagicMock:
    """Create a fully-mocked Gmail API Resource."""
    return MagicMock()


@pytest.fixture
def client(mock_service: MagicMock) -> GmailClient:
    """Create a GmailClient wrapping the mocked service with fast retry settings."""
    return GmailClient(
        mock_service,
        user_id="me",
        max_retries=3,
        initial_backoff_seconds=0.01,
        max_backoff_seconds=0.05,
        inter_page_delay_seconds=0.0,
        num_retries=0,
    )


# ---------- _is_rate_limit_error ----------


class TestIsRateLimitError:
    """Tests for the module-level rate limit detection helper."""

    def test_detects_http_error_429(self) -> None:
        """HttpError with status_code 429 is detected as rate limit."""
        from googleapiclient.errors import HttpError

        exc = HttpError(resp=MagicMock(status=429), content=b"rate limit")
        assert _is_rate_limit_error(exc) is True

    def test_detects_429_in_string(self) -> None:
        """Generic exception with '429' in message is detected."""
        assert _is_rate_limit_error(Exception("HttpError 429: rateLimitExceeded")) is True

    def test_detects_rate_limit_exceeded_in_string(self) -> None:
        """Generic exception with 'rateLimitExceeded' is detected."""
        assert _is_rate_limit_error(Exception("rateLimitExceeded")) is True

    def test_non_rate_limit_error(self) -> None:
        """Non-rate-limit errors return False."""
        assert _is_rate_limit_error(Exception("Server error 500")) is False

    def test_http_error_non_429(self) -> None:
        """HttpError with non-429 status is not a rate limit."""
        from googleapiclient.errors import HttpError

        exc = HttpError(resp=MagicMock(status=500), content=b"server error")
        assert _is_rate_limit_error(exc) is False


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

    def test_retries_on_429_then_succeeds(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """list_labels() retries on 429 and returns on success."""
        mock_exec = mock_service.users().labels().list().execute
        mock_exec.side_effect = [
            Exception("HttpError 429: rateLimitExceeded"),
            {"labels": [{"id": "INBOX", "name": "INBOX"}]},
        ]

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
            result = client.list_labels()

        assert result == [{"id": "INBOX", "name": "INBOX"}]

    def test_raises_rate_limit_after_exhausted_retries(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """list_labels() raises RateLimitError after max_retries exhausted."""
        mock_exec = mock_service.users().labels().list().execute
        mock_exec.side_effect = Exception("HttpError 429: rateLimitExceeded")

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
            with pytest.raises(RateLimitError, match="Rate limited during list labels"):
                client.list_labels()

        # Should have been called max_retries + 1 times (initial + retries)
        assert mock_exec.call_count == 4  # 1 initial + 3 retries


# ---------- _execute_with_retry ----------


class TestExecuteWithRetry:
    """Tests for the _execute_with_retry method."""

    def test_passes_num_retries_to_execute(
        self, mock_service: MagicMock
    ) -> None:
        """num_retries is passed through to request.execute()."""
        client = GmailClient(
            mock_service, num_retries=7, initial_backoff_seconds=0.01,
            max_retries=1, inter_page_delay_seconds=0,
        )
        mock_request = MagicMock()
        mock_request.execute.return_value = {"result": "ok"}

        client._execute_with_retry(mock_request, "test")

        mock_request.execute.assert_called_once_with(num_retries=7)

    def test_exponential_backoff_increases(
        self, mock_service: MagicMock
    ) -> None:
        """Backoff doubles on each retry until max_backoff is hit."""
        client = GmailClient(
            mock_service,
            max_retries=3,
            initial_backoff_seconds=1.0,
            max_backoff_seconds=10.0,
            inter_page_delay_seconds=0,
        )
        mock_request = MagicMock()
        mock_request.execute.side_effect = [
            Exception("429 rateLimitExceeded"),
            Exception("429 rateLimitExceeded"),
            Exception("429 rateLimitExceeded"),
            {"result": "ok"},
        ]

        sleep_times: list[float] = []

        with patch("gmail_ingestor.core.gmail_client.time.sleep", side_effect=lambda t: sleep_times.append(t)):
            with patch("gmail_ingestor.core.gmail_client.random.uniform", side_effect=lambda a, b: b):
                result = client._execute_with_retry(mock_request, "test")

        assert result == {"result": "ok"}
        # Backoff: 1.0, 2.0, 4.0 (all under max of 10.0)
        assert sleep_times == [1.0, 2.0, 4.0]

    def test_backoff_capped_at_max(
        self, mock_service: MagicMock
    ) -> None:
        """Backoff is capped at max_backoff_seconds."""
        client = GmailClient(
            mock_service,
            max_retries=4,
            initial_backoff_seconds=2.0,
            max_backoff_seconds=5.0,
            inter_page_delay_seconds=0,
        )
        mock_request = MagicMock()
        mock_request.execute.side_effect = [
            Exception("429"),
            Exception("429"),
            Exception("429"),
            Exception("429"),
            {"ok": True},
        ]

        sleep_times: list[float] = []

        with patch("gmail_ingestor.core.gmail_client.time.sleep", side_effect=lambda t: sleep_times.append(t)):
            with patch("gmail_ingestor.core.gmail_client.random.uniform", side_effect=lambda a, b: b):
                client._execute_with_retry(mock_request, "test")

        # Backoff: 2.0, 4.0, 5.0 (capped), 5.0 (capped)
        assert sleep_times == [2.0, 4.0, 5.0, 5.0]

    def test_jitter_applied(self, mock_service: MagicMock) -> None:
        """Sleep time uses random jitter between 0 and backoff."""
        client = GmailClient(
            mock_service,
            max_retries=1,
            initial_backoff_seconds=4.0,
            max_backoff_seconds=10.0,
            inter_page_delay_seconds=0,
        )
        mock_request = MagicMock()
        mock_request.execute.side_effect = [
            Exception("429"),
            {"ok": True},
        ]

        with patch("gmail_ingestor.core.gmail_client.time.sleep") as mock_sleep:
            with patch("gmail_ingestor.core.gmail_client.random.uniform", return_value=2.5):
                client._execute_with_retry(mock_request, "test")

        mock_sleep.assert_called_once_with(2.5)

    def test_non_429_error_propagates_immediately(
        self, mock_service: MagicMock
    ) -> None:
        """Non-rate-limit errors are not retried."""
        client = GmailClient(
            mock_service, max_retries=3, initial_backoff_seconds=0.01,
            inter_page_delay_seconds=0,
        )
        mock_request = MagicMock()
        mock_request.execute.side_effect = Exception("Server error 500")

        with pytest.raises(GmailIngestorError, match="Failed to test"):
            client._execute_with_retry(mock_request, "test")

        # Should only be called once — no retries for non-429
        assert mock_request.execute.call_count == 1


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

    def test_retries_on_429_during_discovery(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() retries on 429 via _execute_with_retry."""
        mock_exec = mock_service.users().messages().list().execute
        mock_exec.side_effect = [
            Exception("429 rateLimitExceeded"),
            {"messages": [{"id": "msg1", "threadId": "t1"}]},
        ]

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
            pages = list(client.discover_message_ids("INBOX"))

        assert len(pages) == 1
        assert pages[0][0].message_id == "msg1"

    def test_raises_rate_limit_after_exhausted_retries(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() raises RateLimitError after retries exhausted."""
        mock_exec = mock_service.users().messages().list().execute
        mock_exec.side_effect = Exception("429 rateLimitExceeded")

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
            with pytest.raises(RateLimitError, match="Rate limited during discover messages"):
                list(client.discover_message_ids("INBOX"))

    def test_raises_gmail_error_on_generic_api_failure(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """discover_message_ids() wraps non-429 errors in GmailIngestorError."""
        mock_service.users().messages().list().execute.side_effect = Exception("Server error 500")

        with pytest.raises(GmailIngestorError, match="Failed to discover messages"):
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

    def test_inter_page_delay_between_pages(self, mock_service: MagicMock) -> None:
        """Inter-page delay is applied between pages but not before the first."""
        client = GmailClient(
            mock_service,
            inter_page_delay_seconds=0.5,
            max_retries=0,
            initial_backoff_seconds=0.01,
        )
        page1 = {
            "messages": [{"id": "msg1", "threadId": "t1"}],
            "nextPageToken": "tok2",
        }
        page2 = {
            "messages": [{"id": "msg2", "threadId": "t2"}],
        }
        mock_list = mock_service.users().messages().list
        mock_list.return_value.execute.side_effect = [page1, page2]

        with patch("gmail_ingestor.core.gmail_client.time.sleep") as mock_sleep:
            pages = list(client.discover_message_ids("INBOX"))

        assert len(pages) == 2
        # Sleep should be called once (between page 1 and 2, not before page 1)
        mock_sleep.assert_called_once_with(0.5)


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

    def test_retries_batch_on_429_in_callback(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() retries entire batch when callback reports 429."""
        msg1 = {"id": "msg1", "threadId": "t1", "payload": {}}
        call_count = 0

        def fake_new_batch(callback: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            batch = MagicMock()

            def fake_execute() -> None:
                if call_count == 1:
                    callback("msg1", None, Exception("429 rateLimitExceeded"))
                else:
                    callback("msg1", msg1, None)

            batch.execute.side_effect = fake_execute
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
            result = client.fetch_messages_batch(["msg1"])

        assert result == [msg1]
        assert call_count == 2

    def test_raises_rate_limit_after_exhausted_batch_retries(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() raises RateLimitError after max_retries exhausted."""

        def fake_new_batch(callback: Any) -> MagicMock:
            batch = MagicMock()

            def fake_execute() -> None:
                callback("msg1", None, Exception("429 rateLimitExceeded"))

            batch.execute.side_effect = fake_execute
            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
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

    def test_retries_on_429_from_batch_execute_exception(
        self, client: GmailClient, mock_service: MagicMock
    ) -> None:
        """fetch_messages_batch() retries when batch.execute() itself throws a 429."""
        msg1 = {"id": "msg1", "payload": {}}
        call_count = 0

        def fake_new_batch(callback: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            batch = MagicMock()

            if call_count == 1:
                batch.execute.side_effect = Exception("429 rateLimitExceeded")
            else:
                def fake_execute() -> None:
                    callback("msg1", msg1, None)
                batch.execute.side_effect = fake_execute

            return batch

        mock_service.new_batch_http_request.side_effect = fake_new_batch

        with patch("gmail_ingestor.core.gmail_client.time.sleep"):
            result = client.fetch_messages_batch(["msg1"])

        assert result == [msg1]

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
