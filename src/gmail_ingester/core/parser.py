"""Gmail message parser: MIME tree walking, base64url decoding, header extraction."""

from __future__ import annotations

import base64
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

from gmail_ingester.core.exceptions import ParseError
from gmail_ingester.core.models import EmailBody, EmailHeader, EmailMessage

logger = logging.getLogger(__name__)


class GmailParser:
    """Parses raw Gmail API message dicts into EmailMessage objects."""

    def parse(self, raw_message: dict[str, Any]) -> EmailMessage:
        """Parse a raw Gmail API message dict into an EmailMessage.

        Args:
            raw_message: Full message dict from Gmail API (format=full).

        Returns:
            Parsed EmailMessage.

        Raises:
            ParseError: If the message structure is invalid.
        """
        try:
            message_id = raw_message["id"]
            thread_id = raw_message.get("threadId", "")
            label_ids = tuple(raw_message.get("labelIds", []))
            snippet = raw_message.get("snippet", "")
            payload = raw_message.get("payload", {})

            header = self._extract_headers(payload)
            body = self._extract_body(payload)

            return EmailMessage(
                message_id=message_id,
                thread_id=thread_id,
                label_ids=label_ids,
                header=header,
                body=body,
                snippet=snippet,
            )
        except ParseError:
            raise
        except Exception as e:
            raise ParseError(f"Failed to parse message {raw_message.get('id', '?')}: {e}") from e

    def _extract_headers(self, payload: dict[str, Any]) -> EmailHeader:
        """Extract standard email headers from the payload."""
        headers_list = payload.get("headers", [])
        headers: dict[str, str] = {}
        for h in headers_list:
            name = h.get("name", "").lower()
            if name in ("subject", "from", "to", "date", "cc", "message-id"):
                headers[name] = h.get("value", "")

        date_str = headers.get("date", "")
        date = self._parse_date(date_str)

        return EmailHeader(
            subject=headers.get("subject", "(no subject)"),
            sender=headers.get("from", ""),
            to=headers.get("to", ""),
            date=date,
            cc=headers.get("cc", ""),
            message_id_header=headers.get("message-id", ""),
        )

    def _extract_body(self, payload: dict[str, Any]) -> EmailBody:
        """Recursively walk the MIME tree to extract text/html bodies."""
        plain_text: str | None = None
        html: str | None = None

        plain_text, html = self._walk_parts(payload)

        if plain_text is None and html is None:
            # Try the top-level body directly
            body_data = payload.get("body", {}).get("data")
            if body_data:
                mime_type = payload.get("mimeType", "")
                decoded = self._decode_body(body_data)
                if "html" in mime_type:
                    html = decoded
                else:
                    plain_text = decoded

        return EmailBody(plain_text=plain_text, html=html)

    def _walk_parts(self, part: dict[str, Any]) -> tuple[str | None, str | None]:
        """Recursively walk MIME parts to find text/plain and text/html.

        Args:
            part: A MIME part dict from the Gmail API.

        Returns:
            Tuple of (plain_text, html) — either may be None.
        """
        plain_text: str | None = None
        html: str | None = None
        mime_type = part.get("mimeType", "")

        if mime_type == "text/plain":
            data = part.get("body", {}).get("data")
            if data:
                plain_text = self._decode_body(data)
        elif mime_type == "text/html":
            data = part.get("body", {}).get("data")
            if data:
                html = self._decode_body(data)
        elif mime_type.startswith("multipart/"):
            for sub_part in part.get("parts", []):
                # Skip attachments
                if sub_part.get("filename"):
                    continue

                sub_plain, sub_html = self._walk_parts(sub_part)
                if sub_plain and not plain_text:
                    plain_text = sub_plain
                if sub_html and not html:
                    html = sub_html

        return plain_text, html

    @staticmethod
    def _decode_body(data: str) -> str:
        """Decode base64url-encoded body data.

        Args:
            data: Base64url-encoded string from Gmail API.

        Returns:
            Decoded UTF-8 string.
        """
        # Gmail uses base64url encoding (RFC 4648 §5)
        padded = data + "=" * (4 - len(data) % 4) if len(data) % 4 else data
        return base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """Parse an RFC 2822 date string into a datetime.

        Args:
            date_str: Email date header value.

        Returns:
            Parsed datetime, or epoch datetime if parsing fails.
        """
        if not date_str:
            return datetime(1970, 1, 1)
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            logger.warning("Failed to parse date: %s", date_str)
            return datetime(1970, 1, 1)
