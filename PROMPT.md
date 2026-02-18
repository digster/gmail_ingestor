# Prompts

## 2026-02-17

Implement the Gmail Ingester project from the implementation plan: Python utility that fetches Gmail emails by label and converts their HTML/text bodies to markdown using trafilatura. Three-stage pipeline (discovery → fetch → convert) with SQLite tracking, raw email preservation, and generator-based pagination for TUI/GUI readiness.

Write two test files for the Gmail client and ingester. Test GmailClient with mocked Gmail API service (list_labels, discover_message_ids pagination/empty/429, fetch_messages_batch/errors). Test EmailIngester with mocked dependencies (run_discovery pagination, run_fetch_pending parse/store/mark, run_convert_pending convert/write, progress callback, failed message tracking).

Write three test files for the storage layer of gmail-ingester (test_tracker.py, test_raw_store.py, test_writer.py). Test FetchTracker connect/insert_pending/bulk_insert_pending/update_status/get_pending_ids/get_fetched_ids/count_by_status/is_tracked/start_run/complete_run/retry_failed/context manager. Test RawEmailStore store() with text+html, text-only, html-only, file naming, content preservation, return paths. Test MarkdownWriter write(), filename pattern, special characters/unicode slugify, content preservation, _slugify edge cases.
