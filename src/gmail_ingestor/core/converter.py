"""HTML to Markdown converter using trafilatura with plain text fallback."""

from __future__ import annotations

import logging

import trafilatura

from gmail_ingestor.core.exceptions import ConversionError
from gmail_ingestor.core.models import ConvertedEmail, EmailBody, EmailHeader

logger = logging.getLogger(__name__)


class MarkdownConverter:
    """Convert email body content to markdown with YAML front matter."""

    def convert(self, message_id: str, header: EmailHeader, body: EmailBody) -> ConvertedEmail:
        """Convert an email body to markdown.

        Strategy:
        1. If HTML available, convert via trafilatura (favor_recall=True for email layouts).
        2. If trafilatura returns None or no HTML, fall back to plain text.
        3. Wrap the result with YAML front matter.

        Args:
            message_id: Gmail message ID.
            header: Parsed email headers.
            body: Parsed email body.

        Returns:
            ConvertedEmail with full markdown content.

        Raises:
            ConversionError: If conversion completely fails.
        """
        markdown_body = self._convert_body(body)

        if markdown_body is None:
            raise ConversionError(f"No convertible content for message {message_id}")

        front_matter = self._build_front_matter(header)
        full_markdown = f"{front_matter}\n{markdown_body}"

        return ConvertedEmail(
            message_id=message_id,
            markdown=full_markdown,
            header=header,
        )

    def _convert_body(self, body: EmailBody) -> str | None:
        """Attempt to convert body to markdown string."""
        result: str | None = None

        if body.html:
            try:
                result = trafilatura.extract(
                    body.html,
                    output_format="txt",
                    favor_recall=True,
                    include_links=True,
                    include_tables=True,
                )
            except Exception as e:
                logger.warning("Trafilatura extraction failed: %s", e)
                result = None

        if result is None and body.plain_text:
            result = body.plain_text

        return result

    @staticmethod
    def _build_front_matter(header: EmailHeader) -> str:
        """Build YAML front matter from email headers."""
        date_str = header.date.strftime("%Y-%m-%d %H:%M:%S")
        # Escape YAML special characters in strings
        subject = header.subject.replace('"', '\\"')
        sender = header.sender.replace('"', '\\"')
        to = header.to.replace('"', '\\"')

        lines = [
            "---",
            f'subject: "{subject}"',
            f'from: "{sender}"',
            f'to: "{to}"',
            f"date: {date_str}",
        ]
        if header.cc:
            cc = header.cc.replace('"', '\\"')
            lines.append(f'cc: "{cc}"')
        if header.label_names:
            escaped_names = [name.replace('"', '\\"') for name in header.label_names]
            names_str = ", ".join(f'"{n}"' for n in escaped_names)
            lines.append(f"labels: [{names_str}]")
        if header.label_ids:
            escaped_ids = [lid.replace('"', '\\"') for lid in header.label_ids]
            ids_str = ", ".join(f'"{lid}"' for lid in escaped_ids)
            lines.append(f"label_ids: [{ids_str}]")
        lines.append("---")

        return "\n".join(lines)
