"""Pipeline orchestrator: discovery → fetch → convert → write."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from gmail_ingestor.config.settings import GmailIngestorSettings
from gmail_ingestor.core.auth import authenticate, build_gmail_service
from gmail_ingestor.core.converter import MarkdownConverter
from gmail_ingestor.core.exceptions import ConversionError, GmailIngestorError
from gmail_ingestor.core.gmail_client import GmailClient
from gmail_ingestor.core.models import EmailBody, EmailHeader, FetchProgress
from gmail_ingestor.core.parser import GmailParser
from gmail_ingestor.storage.raw_store import RawEmailStore
from gmail_ingestor.storage.tracker import FetchTracker
from gmail_ingestor.storage.writer import MarkdownWriter

logger = logging.getLogger(__name__)


class EmailIngestor:
    """Orchestrates the three-stage email ingestion pipeline.

    Stage 1 - Discovery: Paginate message IDs → filter already-tracked → insert as 'pending'
    Stage 2 - Fetch:     Batch fetch pending → parse MIME → save raw → mark 'fetched'
    Stage 3 - Convert:   Convert body via trafilatura → write .md → mark 'converted'
    """

    def __init__(
        self,
        settings: GmailIngestorSettings | None = None,
        on_progress: Callable[[FetchProgress], None] | None = None,
    ) -> None:
        self._settings = settings or GmailIngestorSettings()
        self._on_progress = on_progress
        self._progress = FetchProgress()

        # Components initialized lazily
        self._client: GmailClient | None = None
        self._parser = GmailParser()
        self._converter = MarkdownConverter()
        self._tracker: FetchTracker | None = None
        self._raw_store: RawEmailStore | None = None
        self._writer: MarkdownWriter | None = None

    @property
    def on_progress(self) -> Callable[[FetchProgress], None] | None:
        return self._on_progress

    @on_progress.setter
    def on_progress(self, callback: Callable[[FetchProgress], None] | None) -> None:
        self._on_progress = callback

    def _ensure_initialized(
        self,
    ) -> tuple[GmailClient, FetchTracker, RawEmailStore, MarkdownWriter]:
        """Initialize all components if not already done."""
        if self._client is None:
            self._settings.ensure_directories()

            creds = authenticate(
                self._settings.credentials_path,
                self._settings.token_path,
            )
            service = build_gmail_service(creds)
            self._client = GmailClient(service)

        if self._tracker is None:
            self._tracker = FetchTracker(self._settings.database_path)
            self._tracker.connect()

        if self._raw_store is None:
            self._raw_store = RawEmailStore(self._settings.output_raw_dir)

        if self._writer is None:
            self._writer = MarkdownWriter(self._settings.output_markdown_dir)

        return self._client, self._tracker, self._raw_store, self._writer

    def run(
        self,
        label_id: str | None = None,
        query: str | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
        batch_size: int | None = None,
    ) -> FetchProgress:
        """Run the full three-stage pipeline.

        Args:
            label_id: Gmail label ID (defaults to settings.label).
            query: Optional Gmail search query.
            limit: Cap total messages discovered (applies to discovery only).
            offset: Skip first N discovered messages (applies to discovery only).
            batch_size: Override batch size for fetch/convert stages.

        Returns:
            FetchProgress with final counts.
        """
        label = label_id or self._settings.label
        client, tracker, raw_store, writer = self._ensure_initialized()

        run_id = tracker.start_run(label)
        self._progress = FetchProgress(current_stage="discovery")
        self._notify()

        try:
            self.run_discovery(label, query=query, limit=limit, offset=offset)
            self.run_fetch_pending(batch_size=batch_size)
            self.run_convert_pending(batch_size=batch_size)

            self._progress.current_stage = "complete"
            self._notify()
        except Exception as e:
            self._progress.current_stage = f"error: {e}"
            self._notify()
            raise
        finally:
            tracker.complete_run(
                run_id,
                ids_discovered=self._progress.ids_discovered,
                messages_fetched=self._progress.messages_fetched,
                messages_converted=self._progress.messages_converted,
                messages_failed=self._progress.messages_failed,
            )

        return self._progress

    def run_discovery(
        self,
        label_id: str | None = None,
        query: str | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> int:
        """Stage 1: Discover message IDs and insert as pending.

        Args:
            label_id: Gmail label ID (defaults to settings.label).
            query: Optional Gmail search query.
            limit: Cap total messages inserted. None means unlimited.
            offset: Skip the first N discovered stubs before inserting.

        Returns the number of newly discovered IDs.
        """
        label = label_id or self._settings.label
        client, tracker, _, _ = self._ensure_initialized()

        self._progress.current_stage = "discovery"
        self._notify()

        total_new = 0
        total_seen = 0
        total_collected = 0

        for page in client.discover_message_ids(
            label, self._settings.max_results_per_page, query=query
        ):
            stubs_to_insert: list[tuple[str, str]] = []

            for stub in page:
                total_seen += 1

                # Skip first `offset` stubs
                if total_seen <= offset:
                    continue

                stubs_to_insert.append((stub.message_id, stub.thread_id))
                total_collected += 1

                if limit is not None and total_collected >= limit:
                    break

            if stubs_to_insert:
                inserted = tracker.bulk_insert_pending(stubs_to_insert, label)
                total_new += inserted
                self._progress.ids_discovered += len(stubs_to_insert)
                self._notify()
                logger.info(
                    "Discovery page: %d IDs (%d new)", len(stubs_to_insert), inserted
                )

            if limit is not None and total_collected >= limit:
                break

        return total_new

    def run_fetch_pending(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        batch_size: int | None = None,
    ) -> int:
        """Stage 2: Fetch pending messages in batches, parse, and save raw content.

        Args:
            limit: Cap total messages fetched. None means unlimited.
            offset: Skip first N pending messages (SQL OFFSET on each query).
            batch_size: Override settings.batch_size for this run.

        Returns the number of messages successfully fetched.
        """
        client, tracker, raw_store, _ = self._ensure_initialized()

        self._progress.current_stage = "fetch"
        self._notify()

        total_fetched = 0
        effective_batch_size = batch_size or self._settings.batch_size

        while True:
            query_limit = effective_batch_size
            if limit is not None:
                remaining = limit - total_fetched
                if remaining <= 0:
                    break
                query_limit = min(effective_batch_size, remaining)

            pending_ids = tracker.get_pending_ids(limit=query_limit, offset=offset)
            if not pending_ids:
                break

            try:
                raw_messages = client.fetch_messages_batch(pending_ids)
            except GmailIngestorError as e:
                logger.error("Batch fetch failed: %s", e)
                break

            fetched_ids = set()
            for raw_msg in raw_messages:
                msg_id = raw_msg.get("id", "")
                try:
                    email = self._parser.parse(raw_msg)

                    # Save raw content
                    raw_paths: dict[str, Any] = {}
                    if email.body:
                        raw_paths = raw_store.store(msg_id, email.body)

                    # Update tracker
                    tracker.update_status(
                        msg_id,
                        "fetched",
                        subject=email.header.subject if email.header else "",
                        sender=email.header.sender if email.header else "",
                        date=email.header.date.isoformat() if email.header else "",
                        raw_text_path=str(raw_paths.get("text", "")),
                        raw_html_path=str(raw_paths.get("html", "")),
                    )
                    fetched_ids.add(msg_id)
                    total_fetched += 1
                    self._progress.messages_fetched += 1
                    self._notify()

                except Exception as e:
                    logger.error("Failed to process message %s: %s", msg_id, e)
                    tracker.update_status(msg_id, "failed", error_message=str(e))
                    self._progress.messages_failed += 1
                    self._notify()

            # Mark any pending IDs that weren't in the batch response as failed
            for msg_id in pending_ids:
                if msg_id not in fetched_ids:
                    record = tracker.get_message(msg_id)
                    if not record or record.get("status") == "pending":
                        tracker.update_status(
                            msg_id, "failed", error_message="Not returned in batch response"
                        )
                        self._progress.messages_failed += 1

        return total_fetched

    def run_convert_pending(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        batch_size: int | None = None,
    ) -> int:
        """Stage 3: Convert fetched messages to markdown and write files.

        Args:
            limit: Cap total messages converted. None means unlimited.
            offset: Skip first N fetched messages (SQL OFFSET on each query).
            batch_size: Override settings.batch_size for this run.

        Returns the number of messages successfully converted.
        """
        _, tracker, raw_store, writer = self._ensure_initialized()

        self._progress.current_stage = "convert"
        self._notify()

        total_converted = 0
        effective_batch_size = batch_size or self._settings.batch_size

        while True:
            query_limit = effective_batch_size
            if limit is not None:
                remaining = limit - total_converted
                if remaining <= 0:
                    break
                query_limit = min(effective_batch_size, remaining)

            fetched_ids = tracker.get_fetched_ids(limit=query_limit, offset=offset)
            if not fetched_ids:
                break

            for msg_id in fetched_ids:
                msg_record = tracker.get_message(msg_id)
                if not msg_record:
                    continue

                try:
                    plain_text = None
                    html = None
                    raw_text_path = msg_record.get("raw_text_path", "")
                    raw_html_path = msg_record.get("raw_html_path", "")

                    if raw_text_path:
                        p = Path(raw_text_path)
                        if p.exists():
                            plain_text = p.read_text(encoding="utf-8")
                    if raw_html_path:
                        p = Path(raw_html_path)
                        if p.exists():
                            html = p.read_text(encoding="utf-8")

                    body = EmailBody(plain_text=plain_text, html=html)

                    date_str = msg_record.get("date", "")
                    try:
                        date = (
                            datetime.fromisoformat(date_str) if date_str else datetime(1970, 1, 1)
                        )
                    except ValueError:
                        date = datetime(1970, 1, 1)

                    header = EmailHeader(
                        subject=msg_record.get("subject", "(no subject)"),
                        sender=msg_record.get("sender", ""),
                        to="",
                        date=date,
                    )

                    converted = self._converter.convert(msg_id, header, body)
                    filepath = writer.write(converted)

                    tracker.update_status(msg_id, "converted", markdown_path=str(filepath))
                    total_converted += 1
                    self._progress.messages_converted += 1
                    self._notify()

                except ConversionError as e:
                    logger.warning("Conversion failed for %s: %s", msg_id, e)
                    tracker.update_status(msg_id, "failed", error_message=str(e))
                    self._progress.messages_failed += 1
                    self._notify()
                except Exception as e:
                    logger.error("Unexpected error converting %s: %s", msg_id, e)
                    tracker.update_status(msg_id, "failed", error_message=str(e))
                    self._progress.messages_failed += 1
                    self._notify()

        return total_converted

    def list_labels(self) -> list[dict[str, str]]:
        """List available Gmail labels."""
        client, _, _, _ = self._ensure_initialized()
        return client.list_labels()

    def get_status(self) -> dict[str, int]:
        """Get current message counts by status."""
        _, tracker, _, _ = self._ensure_initialized()
        return tracker.count_by_status()

    def retry_failed(self) -> int:
        """Reset failed messages to pending for retry."""
        _, tracker, _, _ = self._ensure_initialized()
        return tracker.retry_failed()

    def close(self) -> None:
        """Clean up resources."""
        if self._tracker:
            self._tracker.close()

    def _notify(self) -> None:
        """Send progress update to callback if registered."""
        if self._on_progress:
            self._on_progress(self._progress)
