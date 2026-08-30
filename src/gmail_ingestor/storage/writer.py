"""Markdown file writer with slug/ID naming convention."""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

from gmail_ingestor.core.models import ConvertedEmail

logger = logging.getLogger(__name__)


class MarkdownWriter:
    """Write converted emails to markdown files with structured naming."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, email: ConvertedEmail) -> Path:
        """Write a converted email to a markdown file.

        File naming: {slug}_{message_id}.md
        Example: weekly-newsletter_18a3f2b0deadbeef.md

        The *full* Gmail message ID is used, never a prefix. Gmail message IDs are
        time-ordered, so truncating one discards exactly the bits that distinguish
        emails arriving close together — a truncated ID collides far more often than
        a truncated hash would. Since message_id is the PRIMARY KEY of the messages
        table, the full form makes filename collisions structurally impossible, and
        it matches RawEmailStore's {message_id}.txt / {message_id}.html naming.

        Args:
            email: Converted email with markdown content.

        Returns:
            Path to the written file.
        """
        slug = self._slugify(email.header.subject)
        filename = f"{slug}_{email.message_id}.md"

        filepath = self._output_dir / filename
        filepath.write_text(email.markdown, encoding="utf-8")
        logger.debug("Wrote markdown: %s", filepath)

        return filepath

    @staticmethod
    def _slugify(text: str, max_length: int = 50) -> str:
        """Convert text to a URL-safe slug.

        Args:
            text: Input text (typically email subject).
            max_length: Maximum slug length.

        Returns:
            Lowercase, hyphenated, ASCII-safe slug.
        """
        # Normalize unicode characters
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
        # Lowercase and replace non-alphanum with hyphens
        text = re.sub(r"[^\w\s-]", "", text.lower())
        text = re.sub(r"[-\s]+", "-", text).strip("-")
        return text[:max_length] if text else "untitled"
