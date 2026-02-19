"""Raw email content storage — saves original text/HTML to files."""

from __future__ import annotations

import logging
from pathlib import Path

from gmail_ingestor.core.models import EmailBody

logger = logging.getLogger(__name__)


class RawEmailStore:
    """Store original email body content (text/plain and text/html) to disk."""

    def __init__(self, raw_dir: Path) -> None:
        self._raw_dir = raw_dir
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def store(self, message_id: str, body: EmailBody) -> dict[str, Path]:
        """Save the original email body content to files.

        Args:
            message_id: Gmail message ID (used as filename base).
            body: Parsed email body.

        Returns:
            Dict with 'text' and/or 'html' keys mapping to saved file paths.
        """
        saved: dict[str, Path] = {}

        if body.plain_text:
            text_path = self._raw_dir / f"{message_id}.txt"
            text_path.write_text(body.plain_text, encoding="utf-8")
            saved["text"] = text_path
            logger.debug("Saved raw text: %s", text_path)

        if body.html:
            html_path = self._raw_dir / f"{message_id}.html"
            html_path.write_text(body.html, encoding="utf-8")
            saved["html"] = html_path
            logger.debug("Saved raw HTML: %s", html_path)

        return saved
