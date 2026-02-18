"""Gmail API client for listing labels, discovering messages, and batch fetching."""

from __future__ import annotations

import logging
from collections.abc import Generator
from typing import Any

from googleapiclient.discovery import Resource
from googleapiclient.http import BatchHttpRequest

from gmail_ingester.core.exceptions import GmailIngesterError, RateLimitError
from gmail_ingester.core.models import MessageStub

logger = logging.getLogger(__name__)


class GmailClient:
    """Thin wrapper around Gmail API for label listing, message discovery, and batch fetch."""

    def __init__(self, service: Resource, user_id: str = "me") -> None:
        self._service = service
        self._user_id = user_id

    def list_labels(self) -> list[dict[str, str]]:
        """List all Gmail labels.

        Returns:
            List of dicts with 'id' and 'name' keys.
        """
        try:
            results = self._service.users().labels().list(userId=self._user_id).execute()
            labels = results.get("labels", [])
            return [{"id": lbl["id"], "name": lbl["name"]} for lbl in labels]
        except Exception as e:
            raise GmailIngesterError(f"Failed to list labels: {e}") from e

    def discover_message_ids(
        self,
        label_id: str,
        max_results_per_page: int = 100,
        query: str | None = None,
    ) -> Generator[list[MessageStub], None, None]:
        """Paginate through message IDs for a label, yielding pages of MessageStub.

        This is a generator — consumers control the pace of pagination.

        Args:
            label_id: Gmail label ID to filter by.
            max_results_per_page: Number of messages per page (1-500).
            query: Optional Gmail search query to further filter.

        Yields:
            Lists of MessageStub objects, one list per API page.
        """
        page_token: str | None = None

        while True:
            try:
                kwargs: dict[str, Any] = {
                    "userId": self._user_id,
                    "labelIds": [label_id],
                    "maxResults": max_results_per_page,
                }
                if page_token:
                    kwargs["pageToken"] = page_token
                if query:
                    kwargs["q"] = query

                response = self._service.users().messages().list(**kwargs).execute()
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "rateLimitExceeded" in error_str:
                    raise RateLimitError(f"Rate limited during discovery: {e}") from e
                raise GmailIngesterError(f"Failed to list messages: {e}") from e

            messages = response.get("messages", [])
            if not messages:
                return

            stubs = [
                MessageStub(message_id=msg["id"], thread_id=msg["threadId"]) for msg in messages
            ]
            logger.debug("Discovered %d message IDs (page)", len(stubs))
            yield stubs

            page_token = response.get("nextPageToken")
            if not page_token:
                return

    def fetch_messages_batch(self, message_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch full message bodies in a single batch request.

        Args:
            message_ids: List of Gmail message IDs to fetch.

        Returns:
            List of raw Gmail API message dicts.
        """
        results: list[dict[str, Any]] = []
        errors: list[str] = []

        def _callback(
            request_id: str,
            response: dict[str, Any] | None,
            exception: Exception | None,
        ) -> None:
            if exception:
                error_str = str(exception)
                if "429" in error_str or "rateLimitExceeded" in error_str:
                    errors.append(f"Rate limited for {request_id}: {exception}")
                else:
                    logger.warning("Batch fetch error for %s: %s", request_id, exception)
                    errors.append(f"Error for {request_id}: {exception}")
            elif response:
                results.append(response)

        batch: BatchHttpRequest = self._service.new_batch_http_request(callback=_callback)

        for msg_id in message_ids:
            batch.add(
                self._service.users()
                .messages()
                .get(
                    userId=self._user_id,
                    id=msg_id,
                    format="full",
                )
            )

        try:
            batch.execute()
        except Exception as e:
            raise GmailIngesterError(f"Batch request failed: {e}") from e

        if errors:
            rate_limit_errors = [e for e in errors if "Rate limited" in e]
            if rate_limit_errors:
                raise RateLimitError(
                    f"Rate limited during batch fetch ({len(rate_limit_errors)} requests)"
                )
            logger.warning("Batch had %d errors out of %d requests", len(errors), len(message_ids))

        logger.debug("Batch fetched %d messages", len(results))
        return results
