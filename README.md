# Gmail Ingestor

Fetch Gmail emails by label and convert their HTML/text bodies to clean markdown. Built as a core library for future TUI/GUI layers.

## Features

- **Three-stage pipeline**: Discovery → Fetch → Convert, each independently resumable
- **SQLite tracking**: Crash-safe state tracking with dedup and audit history
- **Raw preservation**: Original email text/HTML saved alongside converted markdown
- **Generator-based pagination**: Consumers control fetch pace for progress reporting
- **Trafilatura conversion**: HTML → text with `favor_recall=True` for email layouts
- **YAML front matter**: Each markdown file includes subject, from, to, date, labels metadata
- **Progress callbacks**: `on_progress` hook for real-time TUI/GUI updates
- **Rate limiting & retry**: Exponential backoff with jitter on 429 errors, inter-batch/inter-page delays
- **Incremental sync**: Uses Gmail `history.list` API to discover only new messages since last run
- **Multi-label support**: Comma-separated `--label` flag (e.g. `--label "INBOX,SENT"`)
- **CLI pagination**: `--limit`, `--offset`, `--batch-size`, `--full-sync` flags for controlled runs

## Setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Gmail API credentials (OAuth 2.0 client)

### Installation

```bash
# Clone and install
cd gmail-ingestor
uv sync --dev
```

### Gmail API Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project and enable the **Gmail API**
3. Create OAuth 2.0 credentials (Desktop application type)
4. Download the JSON file to `credentials/client_secret.json`

### Configuration

```bash
cp .env.example .env
# Edit .env with your settings
```

Key settings (all prefixed with `GMAIL_`):

| Variable | Default | Description |
|---|---|---|
| `GMAIL_CREDENTIALS_PATH` | `credentials/client_secret.json` | OAuth credentials |
| `GMAIL_TOKEN_PATH` | `credentials/token.json` | Cached auth token |
| `GMAIL_LABEL` | `INBOX` | Label to fetch |
| `GMAIL_BATCH_SIZE` | `50` | Messages per batch |
| `GMAIL_MAX_RESULTS_PER_PAGE` | `100` | IDs per discovery page |
| `GMAIL_OUTPUT_MARKDOWN_DIR` | `output/markdown` | Markdown output directory |
| `GMAIL_OUTPUT_RAW_DIR` | `output/raw` | Raw email output directory |
| `GMAIL_DATABASE_PATH` | `data/gmail_ingestor.db` | SQLite database path |
| `GMAIL_MAX_RETRIES` | `5` | Max retry attempts on 429 rate limit |
| `GMAIL_INITIAL_BACKOFF_SECONDS` | `1.0` | Starting backoff for retries |
| `GMAIL_MAX_BACKOFF_SECONDS` | `60.0` | Backoff cap |
| `GMAIL_INTER_BATCH_DELAY_SECONDS` | `1.0` | Pause between fetch batches |
| `GMAIL_INTER_PAGE_DELAY_SECONDS` | `0.2` | Pause between discovery pages |
| `GMAIL_NUM_RETRIES` | `3` | Built-in retries for 5xx/transport errors |
| `GMAIL_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, etc.) |

## Usage

### CLI

```bash
# List available Gmail labels
uv run python scripts/cli.py list-labels

# Fetch and convert all emails from a label
uv run python scripts/cli.py fetch --label INBOX

# Fetch from multiple labels
uv run python scripts/cli.py fetch --label "INBOX,SENT"

# Fetch with a search query
uv run python scripts/cli.py fetch --label INBOX --query "from:newsletter@example.com"

# Force full re-scan (skip incremental sync)
uv run python scripts/cli.py fetch --label INBOX --full-sync

# Run individual stages
uv run python scripts/cli.py discover --label INBOX
uv run python scripts/cli.py discover --label INBOX --full-sync
uv run python scripts/cli.py fetch-pending
uv run python scripts/cli.py convert-pending

# Pagination: limit, offset, and batch-size
uv run python scripts/cli.py discover --label INBOX --limit 10
uv run python scripts/cli.py fetch-pending --limit 5 --batch-size 10
uv run python scripts/cli.py fetch --label INBOX --limit 20 --offset 50 --batch-size 25

# Check processing status
uv run python scripts/cli.py status

# Retry failed messages
uv run python scripts/cli.py retry
```

### Library API

```python
from gmail_ingestor import EmailIngestor, FetchProgress
from gmail_ingestor.config.settings import GmailIngestorSettings

settings = GmailIngestorSettings()

def on_progress(progress: FetchProgress):
    print(f"Stage: {progress.current_stage}, Fetched: {progress.messages_fetched}")

ingestor = EmailIngestor(settings=settings, on_progress=on_progress)

# List labels
labels = ingestor.list_labels()

# Run full pipeline
progress = ingestor.run(label_id="INBOX")

# Run with pagination controls
progress = ingestor.run(label_id="INBOX", limit=20, offset=50, batch_size=25)

# Or run stages independently
ingestor.run_discovery(label_id="INBOX", limit=10, offset=5)
ingestor.run_fetch_pending(limit=5, batch_size=10)
ingestor.run_convert_pending(limit=5, batch_size=10)

ingestor.close()
```

## Output

Markdown files are written to `output/markdown/` with the naming convention:

```
{slug}_{message_id}.md
# Example: weekly-newsletter_18a3f2b0deadbeef.md
```

Each file includes YAML front matter:

```yaml
---
id: "18a3f2b0deadbeef"
subject: "Weekly Newsletter"
from: "newsletter@example.com"
to: "you@gmail.com"
date: 2024-01-15 10:30:00
labels: ["Inbox", "Newsletters"]
label_ids: ["INBOX", "Label_42"]
---
```

The **full** Gmail message ID is used in both the filename and the `id` front-matter
field. Gmail IDs are time-ordered, so any truncated prefix collides for emails that
arrive close together — and because `message_id` is the primary key of the `messages`
table, the full form makes filename collisions impossible. The `id` field means
downstream consumers can identify a message from the file's contents alone, without
parsing the filename.

Raw email content (original text/HTML) is preserved in `output/raw/` as
`{message_id}.txt` and `{message_id}.html`, matching the markdown suffix exactly.

### Migrating older output

Output written before this convention used `{slug}_{message_id[:8]}.md` and had no `id`
front-matter field. `scripts/migrate_full_message_ids.py` renames those files, updates
`messages.markdown_path`, and backfills the `id` field — driven entirely by the local
SQLite DB, with no Gmail API calls and no re-conversion:

```bash
uv run python scripts/migrate_full_message_ids.py              # dry run (default)
uv run python scripts/migrate_full_message_ids.py --apply
uv run python scripts/migrate_full_message_ids.py --verify
```

It backs the DB up first and writes a journal that `--rollback <journal>` can replay in
reverse. Every phase is idempotent, so it is safe to re-run after an interruption.

`--repair-front-matter` is a separate pass that rewrites front matter a YAML parser
rejects; add `--apply-repair` to write the fixes. It exists because the converter used
to escape quotes but not backslashes, so headers containing a backslash produced
unparseable YAML that downstream tools skipped outright.

## Development

```bash
# Run tests
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest tests/ --cov=gmail_ingestor --cov-report=term-missing

# Lint
uv run ruff check src/ tests/

# Format
uv run ruff format src/ tests/

# Type check
uv run mypy src/
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture documentation.
