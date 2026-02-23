# gmail-ingestor

Fetch Gmail emails by label and convert HTML/text bodies to clean markdown via a 3-stage pipeline.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Gmail API OAuth 2.0 credentials (Desktop application type)

## Quick Setup

```bash
uv sync --dev
cp .env.example .env          # edit with your settings
# Place OAuth credentials at credentials/client_secret.json
```

First run triggers OAuth browser flow; token is cached at `credentials/token.json`.

## CLI Reference

All commands: `uv run python scripts/cli.py <command>`

| Command | Description | Key Flags |
|---|---|---|
| `list-labels` | List all Gmail labels | — |
| `fetch` | Full pipeline (discover + fetch + convert) | `--label`, `--query`, `--limit`, `--offset`, `--batch-size` |
| `discover` | Stage 1: discover message IDs only | `--label`, `--query`, `--limit`, `--offset` |
| `fetch-pending` | Stage 2: fetch discovered messages | `--limit`, `--offset`, `--batch-size` |
| `convert-pending` | Stage 3: convert fetched to markdown | `--limit`, `--offset`, `--batch-size` |
| `status` | Show message counts by status | — |
| `retry` | Reset failed messages to pending | — |

### Flag Details

| Flag | Type | Default | Description |
|---|---|---|---|
| `--label`, `-l` | string | from `.env` | Gmail label ID |
| `--query`, `-q` | string | — | Gmail search query |
| `--limit` | int | unlimited | Cap total messages processed |
| `--offset` | int | `0` | Skip first N messages |
| `--batch-size` | int | from `.env` | Override batch size for fetch/convert |

## Python API

```python
from gmail_ingestor import EmailIngestor, FetchProgress
from gmail_ingestor.config.settings import GmailIngestorSettings

settings = GmailIngestorSettings()
ingestor = EmailIngestor(settings=settings, on_progress=lambda p: print(p))

# Full pipeline
progress = ingestor.run(label_id="INBOX", limit=20, offset=0, batch_size=25)

# Individual stages
ingestor.run_discovery(label_id="INBOX", limit=10, offset=5)
ingestor.run_fetch_pending(limit=5, batch_size=10)
ingestor.run_convert_pending(limit=5, batch_size=10)

# Utilities
labels = ingestor.list_labels()        # list[dict] with id, name
counts = ingestor.get_status()          # dict[str, int] by status
reset  = ingestor.retry_failed()        # int: count reset to pending

ingestor.close()
```

### Public Exports (`gmail_ingestor`)

`EmailIngestor`, `FetchProgress`, `EmailMessage`, `EmailHeader`, `EmailBody`, `ConvertedEmail`, `MessageStub`

## Pipeline Architecture

```
Stage 1 — Discovery    Gmail API → paginate message IDs → dedup → insert as 'pending'
Stage 2 — Fetch        Batch API fetch → parse MIME → save raw text/html → mark 'fetched'
Stage 3 — Convert      trafilatura HTML→text → write .md with YAML front matter → mark 'converted'
```

State machine: `pending → fetched → converted` (failures go to `failed`, `retry` resets to `pending`).

Each stage is independently resumable — crash mid-fetch and re-run picks up where it left off.

## Output Format

**Markdown** (`output/markdown/`): `{date}_{slug}_{id}.md`

```yaml
---
subject: "Weekly Newsletter"
from: "newsletter@example.com"
to: "you@gmail.com"
date: 2024-01-15 10:30:00
---
# Converted body content here...
```

**Raw** (`output/raw/`): `{id}.txt` and `{id}.html` — original email bodies preserved for re-conversion.

**SQLite** (`data/gmail_ingestor.db`): `messages` table (status tracking + dedup), `fetch_runs` table (audit history).

## Configuration

All env vars use the `GMAIL_` prefix. Loaded from `.env` via pydantic-settings.

| Variable | Default | Description |
|---|---|---|
| `GMAIL_CREDENTIALS_PATH` | `credentials/client_secret.json` | OAuth credentials file |
| `GMAIL_TOKEN_PATH` | `credentials/token.json` | Cached auth token |
| `GMAIL_LABEL` | `INBOX` | Default label to fetch |
| `GMAIL_BATCH_SIZE` | `50` | Messages per API batch |
| `GMAIL_MAX_RESULTS_PER_PAGE` | `100` | IDs per discovery page |
| `GMAIL_OUTPUT_MARKDOWN_DIR` | `output/markdown` | Markdown output directory |
| `GMAIL_OUTPUT_RAW_DIR` | `output/raw` | Raw email output directory |
| `GMAIL_DATABASE_PATH` | `data/gmail_ingestor.db` | SQLite database path |
| `GMAIL_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

## Testing

```bash
uv run pytest tests/ -v                                        # all tests
uv run pytest tests/ --cov=gmail_ingestor --cov-report=term-missing  # with coverage
uv run ruff check src/ tests/                                  # lint
uv run ruff format src/ tests/                                 # format
uv run mypy src/                                               # type check
```

## Common Recipes

```bash
# Ingest first 50 emails from INBOX
uv run python scripts/cli.py fetch --label INBOX --limit 50

# Ingest emails matching a search query
uv run python scripts/cli.py fetch --label INBOX --query "from:newsletter@example.com after:2024/01/01"

# Resume after a crash (picks up pending/fetched items)
uv run python scripts/cli.py fetch-pending
uv run python scripts/cli.py convert-pending

# Retry all failed messages
uv run python scripts/cli.py retry
uv run python scripts/cli.py fetch-pending

# Discover IDs only, then fetch in small batches
uv run python scripts/cli.py discover --label INBOX
uv run python scripts/cli.py fetch-pending --batch-size 10 --limit 20
uv run python scripts/cli.py convert-pending

# Check pipeline progress
uv run python scripts/cli.py status

# Skip first 100 messages, fetch next 50
uv run python scripts/cli.py fetch --label INBOX --offset 100 --limit 50
```
