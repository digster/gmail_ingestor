"""Gmail API client for listing labels, discovering messages, and batch fetching."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Generator
from typing import Any

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

from gmail_ingestor.core.exceptions import GmailIngestorError, RateLimitError
from gmail_ingestor.core.models import MessageStub

logger = logging.getLogger(__name__)


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check whether an exception represents a Gmail API 429 rate limit."""
    if isinstance(exc, HttpError) and exc.status_code == 429:
        return True
    error_str = str(exc)
    return "429" in error_str or "rateLimitExceeded" in error_str


class GmailClient:
    """Thin wrapper around Gmail API for label listing, message discovery, and batch fetch."""

    def __init__(
        self,
        service: Resource,
        user_id: str = "me",
        *,
        max_retries: int = 5,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        inter_page_delay_seconds: float = 0.2,
        num_retries: int = 3,
    ) -> None:
        self._service = service
        self._user_id = user_id
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._inter_page_delay = inter_page_delay_seconds
        self._num_retries = num_retries

    def _execute_with_retry(self, request: Any, context: str) -> Any:
        """Execute a single API request with exponential backoff on 429 errors.

        Args:
            request: A googleapiclient HttpRequest object.
            context: Description for log messages (e.g. "list labels").

        Returns:
            The API response dict.

        Raises:
            RateLimitError: When retries are exhausted on 429 errors.
            GmailIngestorError: On non-rate-limit API errors.
        """
        backoff = self._initial_backoff

        for attempt in range(self._max_retries + 1):
            try:
                return request.execute(num_retries=self._num_retries)
            except Exception as e:
                if _is_rate_limit_error(e):
                    if attempt >= self._max_retries:
                        raise RateLimitError(
                            f"Rate limited during {context} after "
                            f"{self._max_retries} retries: {e}"
                        ) from e
                    sleep_time = min(backoff, self._max_backoff)
                    jitter = random.uniform(0, sleep_time)
                    logger.warning(
                        "Rate limited during %s (attempt %d/%d), "
                        "sleeping %.2fs (backoff=%.2f + jitter=%.2f)",
                        context, attempt + 1, self._max_retries,
                        jitter, backoff, jitter,
                    )
                    time.sleep(jitter)
                    backoff = min(backoff * 2, self._max_backoff)
                else:
                    raise GmailIngestorError(
                        f"Failed to {context}: {e}"
                    ) from e

        # Should not be reached, but just in case
        raise RateLimitError(f"Rate limited during {context} after {self._max_retries} retries")

    def list_labels(self) -> list[dict[str, str]]:
        """List all Gmail labels.

        Returns:
            List of dicts with 'id' and 'name' keys.
        """
        request = self._service.users().labels().list(userId=self._user_id)
        results = self._execute_with_retry(request, "list labels")
        labels = results.get("labels", [])
        return [{"id": lbl["id"], "name": lbl["name"]} for lbl in labels]

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
        first_page = True

        while True:
            if not first_page and self._inter_page_delay > 0:
                time.sleep(self._inter_page_delay)
            first_page = False

            kwargs: dict[str, Any] = {
                "userId": self._user_id,
                "labelIds": [label_id],
                "maxResults": max_results_per_page,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            if query:
                kwargs["q"] = query

            request = self._service.users().messages().list(**kwargs)
            response = self._execute_with_retry(request, "discover messages")

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
        backoff = self._initial_backoff

        for attempt in range(self._max_retries + 1):
            results: list[dict[str, Any]] = []
            errors: list[str] = []
            rate_limited = False

            def _callback(
                request_id: str,
                response: dict[str, Any] | None,
                exception: Exception | None,
            ) -> None:
                nonlocal rate_limited
                if exception:
                    if _is_rate_limit_error(exception):
                        rate_limited = True
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
                if _is_rate_limit_error(e):
                    rate_limited = True
                else:
                    raise GmailIngestorError(f"Batch request failed: {e}") from e

            if rate_limited:
                if attempt >= self._max_retries:
                    raise RateLimitError(
                        f"Rate limited during batch fetch after {self._max_retries} retries"
                    )
                sleep_time = min(backoff, self._max_backoff)
                jitter = random.uniform(0, sleep_time)
                logger.warning(
                    "Rate limited during batch fetch (attempt %d/%d), sleeping %.2fs",
                    attempt + 1, self._max_retries, jitter,
                )
                time.sleep(jitter)
                backoff = min(backoff * 2, self._max_backoff)
                continue

            if errors:
                logger.warning(
                    "Batch had %d errors out of %d requests",
                    len(errors), len(message_ids),
                )

            logger.debug("Batch fetched %d messages", len(results))
            return results

        raise RateLimitError(
            f"Rate limited during batch fetch after {self._max_retries} retries"
        )
