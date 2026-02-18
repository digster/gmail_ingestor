"""Configuration via pydantic-settings with .env support."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class GmailIngesterSettings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_prefix="GMAIL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # OAuth credentials
    credentials_path: Path = Path("credentials/client_secret.json")
    token_path: Path = Path("credentials/token.json")

    # Gmail API settings
    label: str = "INBOX"
    max_results_per_page: int = 100
    batch_size: int = 50

    # Output paths
    output_markdown_dir: Path = Path("output/markdown")
    output_raw_dir: Path = Path("output/raw")

    # Database
    database_path: Path = Path("data/gmail_ingester.db")

    # Logging
    log_level: str = "INFO"

    def ensure_directories(self) -> None:
        """Create output and data directories if they don't exist."""
        self.output_markdown_dir.mkdir(parents=True, exist_ok=True)
        self.output_raw_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.credentials_path.parent.mkdir(parents=True, exist_ok=True)
