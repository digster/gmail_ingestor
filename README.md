# Gmail Ingestor

Fetch Gmail emails by label and convert their HTML/text bodies to clean markdown. Built as a core library for future TUI/GUI layers.

## Features

- **Three-stage pipeline**: Discovery → Fetch → Convert, each independently resumable
- **SQLite tracking**: Crash-safe state tracking with dedup and audit history
- **Raw preservation**: Original email text/HTML saved alongside converted markdown
- **Generator-based pagination**: Consumers control fetch pace for progress reporting
- **Trafilatura conversion**: HTML → text with `favor_recall=True` for email layouts
- **YAML front matter**: Each markdown file includes subject, from, to, date metadata
- **Progress callbacks**: `on_progress` hook for real-time TUI/GUI updates
- **CLI pagination**: `--limit`, `--offset`, `--batch-size` flags for controlled runs

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
| `GMAIL_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, etc.) |

## Usage

### CLI

```bash
# List available Gmail labels
uv run python scripts/cli.py list-labels

# Fetch and convert all emails from a label
uv run python scripts/cli.py fetch --label INBOX

# Fetch with a search query
uv run python scripts/cli.py fetch --label INBOX --query "from:newsletter@example.com"

# Run individual stages
uv run python scripts/cli.py discover --label INBOX
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
{date}_{slug}_{id}.md
# Example: 2024-01-15_weekly-newsletter_18a3f2b.md
```

Each file includes YAML front matter:

```yaml
---
subject: "Weekly Newsletter"
from: "newsletter@example.com"
to: "you@gmail.com"
date: 2024-01-15 10:30:00
---
```

Raw email content (original text/HTML) is preserved in `output/raw/`.

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
