# Prompts

## 2026-02-17

Implement the Gmail Ingestor project from the implementation plan: Python utility that fetches Gmail emails by label and converts their HTML/text bodies to markdown using trafilatura. Three-stage pipeline (discovery → fetch → convert) with SQLite tracking, raw email preservation, and generator-based pagination for TUI/GUI readiness.

Write two test files for the Gmail client and ingestor. Test GmailClient with mocked Gmail API service (list_labels, discover_message_ids pagination/empty/429, fetch_messages_batch/errors). Test EmailIngestor with mocked dependencies (run_discovery pagination, run_fetch_pending parse/store/mark, run_convert_pending convert/write, progress callback, failed message tracking).

Write three test files for the storage layer of gmail-ingestor (test_tracker.py, test_raw_store.py, test_writer.py). Test FetchTracker connect/insert_pending/bulk_insert_pending/update_status/get_pending_ids/get_fetched_ids/count_by_status/is_tracked/start_run/complete_run/retry_failed/context manager. Test RawEmailStore store() with text+html, text-only, html-only, file naming, content preservation, return paths. Test MarkdownWriter write(), filename pattern, special characters/unicode slugify, content preservation, _slugify edge cases.

## 2026-02-17 (session 2)

Add CLI pagination flags (--limit, --offset, --batch-size) to the gmail-ingestor pipeline. Add offset param to tracker.get_pending_ids()/get_fetched_ids(), add limit/offset/batch_size params to ingestor run_discovery/run_fetch_pending/run_convert_pending/run, wire _add_pagination_args helper to fetch/discover/fetch-pending/convert-pending CLI subparsers with input validation, add tests for tracker offset, ingestor pagination, and CLI argument parsing.

## 2026-02-23

Fix `.env` output directory settings not respected. The `env_prefix="GMAIL_"` in pydantic-settings means all fields expect `GMAIL_`-prefixed env vars, but `.env` defined `OUTPUT_MARKDOWN_DIR`, `OUTPUT_RAW_DIR`, `DATABASE_PATH`, and `LOG_LEVEL` without the prefix — causing them to be silently ignored. Renamed these vars in `.env` and `.env.example` to include the `GMAIL_` prefix, and updated README with the missing env var documentation.

Create SKILL.md — a concise, action-oriented quick-reference guide covering one-liner description, prerequisites, quick setup, CLI reference with all commands/flags, Python API with public exports, 3-stage pipeline architecture, output format (markdown/raw/SQLite), configuration env vars table, testing commands, and common copy-paste recipes. Verified against actual CLI, settings, and package exports.

## 2026-02-25

Save label names in DB and add labels to markdown frontmatter. Added `labels` table (label_id→name lookup synced from Gmail API) and `message_labels` junction table (per-message label associations) to SQLite schema. Added `upsert_labels()`, `insert_message_labels()`, `get_message_labels()` methods to FetchTracker. Added `label_ids` and `label_names` tuple fields to EmailHeader. Updated markdown frontmatter to include `labels:` and `label_ids:` YAML lists. Wired `_sync_labels()` at pipeline start, label persistence in fetch stage, label hydration in convert stage. Added tests for all new functionality.
