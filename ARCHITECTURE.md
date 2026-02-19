# Gmail Ingestor — Architecture

## Overview

Gmail Ingestor is a Python library that fetches emails from Gmail by label and converts their HTML/text bodies to markdown using trafilatura. It is designed as a **core library** for future TUI/GUI layers, with clean API boundaries and zero UI dependencies.

## System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLI / Future TUI/GUI                      │
│                      (scripts/cli.py)                            │
├──────────────────────────────────────────────────────────────────┤
│                      Pipeline Layer                              │
│                  (pipeline/ingestor.py)                           │
│         EmailIngestor: orchestrates 3-stage pipeline             │
│         FetchProgress + on_progress callback for UIs             │
├──────────────┬──────────────┬──────────────┬─────────────────────┤
│  Core Layer  │              │              │   Storage Layer     │
│  auth.py     │ gmail_client │  parser.py   │   tracker.py        │
│  OAuth 2.0   │ .py          │  MIME walk   │   (SQLite state)    │
│  + token     │ List/Discover│  base64url   │   raw_store.py      │
│  caching     │ /BatchFetch  │  decode      │   (text/html files) │
│              │              │              │   writer.py          │
│              │              │ converter.py │   (markdown files)   │
│              │              │ trafilatura  │                     │
├──────────────┴──────────────┴──────────────┴─────────────────────┤
│                      Config Layer                                │
│              (config/settings.py)                                 │
│         GmailIngestorSettings via pydantic-settings              │
└──────────────────────────────────────────────────────────────────┘
```

## Three-Stage Pipeline

The `EmailIngestor.run()` processes emails in mini-batches for bounded memory:

```
Stage 1 - Discovery   │ Paginate message IDs → filter already-tracked → insert as 'pending'
Stage 2 - Fetch        │ Batch API fetch → parse MIME → save raw text/html → mark 'fetched'
Stage 3 - Convert      │ trafilatura HTML→text → write .md file → mark 'converted'
```

Each stage is independently callable (`run_discovery`, `run_fetch_pending`, `run_convert_pending`) for resume/retry scenarios. If the process crashes mid-fetch, re-running picks up from where it left off.

## Key Data Flow

```
Gmail API  →  MessageStub (id, threadId)
           →  Raw JSON (format=full)
           →  GmailParser → EmailMessage (header + body)
           →  RawEmailStore → {id}.txt, {id}.html (original preservation)
           →  MarkdownConverter → ConvertedEmail (YAML front matter + body)
           →  MarkdownWriter → {date}_{slug}_{id}.md
```

## State Machine

Messages progress through statuses tracked in SQLite:

```
pending → fetched → converted
    │         │
    └─────────┴──→ failed → (retry) → pending
```

The `fetch_runs` table provides audit history of each ingestion run.

## Directory Layout

```
src/gmail_ingestor/
├── core/               # Domain logic, zero UI deps
│   ├── models.py       # Frozen dataclasses (MessageStub, EmailHeader, EmailBody, EmailMessage, ConvertedEmail, FetchProgress)
│   ├── exceptions.py   # Exception hierarchy (GmailIngestorError → Auth/RateLimit/Parse/Conversion)
│   ├── auth.py         # OAuth 2.0 with token caching, SCOPES = gmail.readonly
│   ├── gmail_client.py # GmailClient: list_labels, discover_message_ids (generator), fetch_messages_batch
│   ├── parser.py       # GmailParser: recursive MIME walk, base64url decode, header extraction
│   └── converter.py    # MarkdownConverter: trafilatura + fallback + YAML front matter
├── storage/
│   ├── tracker.py      # FetchTracker: SQLite with WAL mode, messages + fetch_runs tables
│   ├── raw_store.py    # RawEmailStore: saves original text/html to output/raw/
│   └── writer.py       # MarkdownWriter: {date}_{slug}_{id}.md naming, Unicode-safe slugify
├── pipeline/
│   └── ingestor.py     # EmailIngestor: 3-stage orchestrator with progress callbacks
└── config/
    └── settings.py     # GmailIngestorSettings via pydantic-settings (GMAIL_ env prefix)
```

## Design Decisions

### Generator-Based Pagination
`GmailClient.discover_message_ids()` yields pages of `MessageStub`. Consumers (TUI/GUI) control the pace — can pause between pages for progress display. Full message bodies are fetched separately in configurable batch sizes using Gmail Batch API.

### SQLite Over JSON
Atomic operations prevent corruption from mid-fetch crashes. O(1) dedup via PRIMARY KEY on `message_id`. WAL mode enables concurrent reads during writes.

### Raw Email Preservation
Original text/html saved to `output/raw/` during Stage 2. Enables re-conversion with different settings, debugging, and future analysis pipelines.

### CLI Pagination (--limit, --offset, --batch-size)
All three pipeline stages accept `limit`, `offset`, and `batch_size` parameters. In the full `run()` pipeline, `limit`/`offset` apply to discovery only (capping what enters the DB), while `batch_size` overrides the configured batch size for fetch and convert stages. For `fetch-pending`/`convert-pending`, `offset` uses SQL `OFFSET` on every query — this is correct because processed items transition out of the pending/fetched pool, keeping skipped rows stable across loop iterations.

### Trafilatura with Recall Bias
`favor_recall=True` captures more content from email table-based layouts. Falls back to plain text body if trafilatura returns None.

## External Dependencies

| Package | Purpose |
|---|---|
| `google-api-python-client` | Gmail API client |
| `google-auth-oauthlib` | OAuth 2.0 flow |
| `trafilatura` | HTML → text extraction |
| `pydantic-settings` | Typed config from env/.env |

## Developer Commands

```bash
uv run pytest tests/ -v              # Run tests
uv run ruff check src/ tests/        # Lint
uv run python scripts/cli.py --help  # CLI usage
```
