# Gmail Ingester

Fetch Gmail emails by label and convert their HTML/text bodies to clean markdown. Built as a core library for future TUI/GUI layers.

## Features

- **Three-stage pipeline**: Discovery → Fetch → Convert, each independently resumable
- **SQLite tracking**: Crash-safe state tracking with dedup and audit history
- **Raw preservation**: Original email text/HTML saved alongside converted markdown
- **Generator-based pagination**: Consumers control fetch pace for progress reporting
- **Trafilatura conversion**: HTML → text with `favor_recall=True` for email layouts
- **YAML front matter**: Each markdown file includes subject, from, to, date metadata
- **Progress callbacks**: `on_progress` hook for real-time TUI/GUI updates

## Setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Gmail API credentials (OAuth 2.0 client)

### Installation

```bash
# Clone and install
cd gmail-ingester
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

# Check processing status
uv run python scripts/cli.py status

# Retry failed messages
uv run python scripts/cli.py retry
```

### Library API

```python
from gmail_ingester import EmailIngester, FetchProgress
from gmail_ingester.config.settings import GmailIngesterSettings

settings = GmailIngesterSettings()

def on_progress(progress: FetchProgress):
    print(f"Stage: {progress.current_stage}, Fetched: {progress.messages_fetched}")

ingester = EmailIngester(settings=settings, on_progress=on_progress)

# List labels
labels = ingester.list_labels()

# Run full pipeline
progress = ingester.run(label_id="INBOX")

# Or run stages independently
ingester.run_discovery(label_id="INBOX")
ingester.run_fetch_pending()
ingester.run_convert_pending()

ingester.close()
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
uv run pytest tests/ --cov=gmail_ingester --cov-report=term-missing

# Lint
uv run ruff check src/ tests/

# Format
uv run ruff format src/ tests/

# Type check
uv run mypy src/
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed architecture documentation.
