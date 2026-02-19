"""Comprehensive unit tests for GmailParser."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from gmail_ingestor.core.exceptions import ParseError
from gmail_ingestor.core.parser import GmailParser


@pytest.fixture
def parser() -> GmailParser:
    """Fresh GmailParser instance."""
    return GmailParser()


# ---------------------------------------------------------------------------
# 1. Simple text email
# ---------------------------------------------------------------------------


class TestSimpleTextEmail:
    """Parsing a plain text/plain message with all standard headers."""

    def test_message_id(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.message_id == "msg_simple_text"

    def test_thread_id(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.thread_id == "thread_001"

    def test_label_ids(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.label_ids == ("INBOX", "UNREAD")

    def test_snippet(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.snippet == "Hello, this is a plain text email."

    def test_subject(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.header.subject == "Plain Text Email"

    def test_sender(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.header.sender == "sender@example.com"

    def test_to(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.header.to == "recipient@example.com"

    def test_cc(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.header.cc == "cc@example.com"

    def test_message_id_header(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.header.message_id_header == "<msg001@example.com>"

    def test_date_parsed(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        expected = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone(timedelta(hours=-5)))
        assert msg.header.date == expected

    def test_body_plain_text(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.body.plain_text == "Hello, this is a plain text email.\n\nBest regards,\nSender"

    def test_body_html_is_none(self, parser: GmailParser, simple_text_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_text_raw)
        assert msg.body.html is None


# ---------------------------------------------------------------------------
# 2. Simple HTML email
# ---------------------------------------------------------------------------


class TestSimpleHtmlEmail:
    """Parsing a text/html message."""

    def test_message_id(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        assert msg.message_id == "msg_simple_html"

    def test_thread_id(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        assert msg.thread_id == "thread_002"

    def test_label_ids(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        assert msg.label_ids == ("INBOX",)

    def test_subject(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        assert msg.header.subject == "HTML Email"

    def test_sender(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        assert msg.header.sender == "newsletter@example.com"

    def test_date_utc(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        expected = datetime(2024, 1, 16, 14, 0, 0, tzinfo=UTC)
        assert msg.header.date == expected

    def test_cc_defaults_empty(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        """HTML fixture has no Cc header; should default to empty string."""
        msg = parser.parse(simple_html_raw)
        assert msg.header.cc == ""

    def test_message_id_header_defaults_empty(
        self, parser: GmailParser, simple_html_raw: dict[str, Any]
    ) -> None:
        """HTML fixture has no Message-ID header; should default to empty string."""
        msg = parser.parse(simple_html_raw)
        assert msg.header.message_id_header == ""

    def test_body_html_content(self, parser: GmailParser, simple_html_raw: dict[str, Any]) -> None:
        msg = parser.parse(simple_html_raw)
        expected_html = (
            "<!DOCTYPE html><html><body><h1>Hello World</h1>"
            "<p>This is an <strong>HTML</strong> email with "
            '<a href="https://example.com">a link</a>.</p></body></html>'
        )
        assert msg.body.html == expected_html

    def test_body_plain_text_is_none(
        self, parser: GmailParser, simple_html_raw: dict[str, Any]
    ) -> None:
        msg = parser.parse(simple_html_raw)
        assert msg.body.plain_text is None


# ---------------------------------------------------------------------------
# 3. Multipart/alternative (text + HTML)
# ---------------------------------------------------------------------------


class TestMultipartAlternative:
    """Parsing multipart/alternative: both text and HTML parts extracted."""

    def test_message_id(self, parser: GmailParser, multipart_alt_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_alt_raw)
        assert msg.message_id == "msg_multipart_alt"

    def test_thread_id(self, parser: GmailParser, multipart_alt_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_alt_raw)
        assert msg.thread_id == "thread_003"

    def test_label_ids(self, parser: GmailParser, multipart_alt_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_alt_raw)
        assert msg.label_ids == ("INBOX", "IMPORTANT")

    def test_subject(self, parser: GmailParser, multipart_alt_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_alt_raw)
        assert msg.header.subject == "Multipart Alternative Email"

    def test_plain_text_extracted(
        self, parser: GmailParser, multipart_alt_raw: dict[str, Any]
    ) -> None:
        msg = parser.parse(multipart_alt_raw)
        assert msg.body.plain_text == "This is the plain text version of the email.\n\nBest regards"

    def test_html_extracted(self, parser: GmailParser, multipart_alt_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_alt_raw)
        expected_html = (
            "<html><body><p>This is the <b>HTML</b> version of the email.</p>"
            "<p>Best regards</p></body></html>"
        )
        assert msg.body.html == expected_html

    def test_both_bodies_present(
        self, parser: GmailParser, multipart_alt_raw: dict[str, Any]
    ) -> None:
        msg = parser.parse(multipart_alt_raw)
        assert msg.body.plain_text is not None
        assert msg.body.html is not None


# ---------------------------------------------------------------------------
# 4. Multipart/mixed with attachment
# ---------------------------------------------------------------------------


class TestMultipartMixedWithAttachment:
    """Parsing multipart/mixed: attachment skipped, body parts extracted."""

    def test_message_id(self, parser: GmailParser, multipart_mixed_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_mixed_raw)
        assert msg.message_id == "msg_multipart_mixed"

    def test_subject(self, parser: GmailParser, multipart_mixed_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_mixed_raw)
        assert msg.header.subject == "Email With Attachment"

    def test_plain_text_extracted(
        self, parser: GmailParser, multipart_mixed_raw: dict[str, Any]
    ) -> None:
        msg = parser.parse(multipart_mixed_raw)
        assert msg.body.plain_text == "Please find the attached document."

    def test_html_extracted(self, parser: GmailParser, multipart_mixed_raw: dict[str, Any]) -> None:
        msg = parser.parse(multipart_mixed_raw)
        expected_html = "<html><body><p>Please find the <b>attached</b> document.</p></body></html>"
        assert msg.body.html == expected_html

    def test_attachment_not_in_body(
        self, parser: GmailParser, multipart_mixed_raw: dict[str, Any]
    ) -> None:
        """The PDF attachment should be silently skipped."""
        msg = parser.parse(multipart_mixed_raw)
        assert "pdf" not in (msg.body.plain_text or "").lower()
        assert "attachmentId" not in (msg.body.html or "")

    def test_both_bodies_present(
        self, parser: GmailParser, multipart_mixed_raw: dict[str, Any]
    ) -> None:
        msg = parser.parse(multipart_mixed_raw)
        assert msg.body.plain_text is not None
        assert msg.body.html is not None


# ---------------------------------------------------------------------------
# 5. Base64url decoding edge cases
# ---------------------------------------------------------------------------


class TestBase64UrlDecoding:
    """Edge cases in _decode_body for base64url encoding."""

    def test_standard_base64url(self, parser: GmailParser) -> None:
        """Basic round-trip: encode then decode."""
        original = "Hello, World!"
        encoded = base64.urlsafe_b64encode(original.encode()).decode().rstrip("=")
        assert parser._decode_body(encoded) == original

    def test_padding_needed_mod2(self, parser: GmailParser) -> None:
        """Input length % 4 == 2 requires two padding characters."""
        original = "ab"
        encoded = base64.urlsafe_b64encode(original.encode()).decode().rstrip("=")
        assert len(encoded) % 4 != 0  # confirm padding was stripped
        assert parser._decode_body(encoded) == original

    def test_padding_needed_mod3(self, parser: GmailParser) -> None:
        """Input length % 4 == 3 requires one padding character."""
        original = "abc"
        encoded = base64.urlsafe_b64encode(original.encode()).decode().rstrip("=")
        assert parser._decode_body(encoded) == original

    def test_already_padded(self, parser: GmailParser) -> None:
        """If the data already has proper padding, decoding should still work."""
        original = "Hello"
        encoded = base64.urlsafe_b64encode(original.encode()).decode()  # keep padding
        assert parser._decode_body(encoded) == original

    def test_url_safe_characters(self, parser: GmailParser) -> None:
        """Base64url uses - and _ instead of + and /."""
        # Bytes that produce + and / in standard base64
        data = bytes([0xFB, 0xFF, 0xFE])
        encoded = base64.urlsafe_b64encode(data).decode().rstrip("=")
        assert "+" not in encoded
        assert "/" not in encoded
        parser._decode_body(encoded).encode("utf-8", errors="replace")
        # We just verify no exception is raised and a string is returned
        assert isinstance(parser._decode_body(encoded), str)

    def test_empty_string(self, parser: GmailParser) -> None:
        """Empty data should decode to empty string."""
        assert parser._decode_body("") == ""

    def test_unicode_content(self, parser: GmailParser) -> None:
        """Non-ASCII UTF-8 content should decode correctly."""
        original = "Cafe\u0301 \u2014 re\u0301sume\u0301"
        encoded = base64.urlsafe_b64encode(original.encode("utf-8")).decode().rstrip("=")
        assert parser._decode_body(encoded) == original

    def test_invalid_utf8_replaced(self, parser: GmailParser) -> None:
        """Invalid UTF-8 bytes should be replaced, not raise."""
        bad_bytes = b"\xff\xfe"
        encoded = base64.urlsafe_b64encode(bad_bytes).decode().rstrip("=")
        result = parser._decode_body(encoded)
        assert "\ufffd" in result  # replacement character


# ---------------------------------------------------------------------------
# 6. Date parsing with fallback
# ---------------------------------------------------------------------------


class TestDateParsing:
    """_parse_date edge cases including fallback for invalid dates."""

    def test_valid_rfc2822_date(self) -> None:
        result = GmailParser._parse_date("Mon, 15 Jan 2024 10:30:00 -0500")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_valid_utc_date(self) -> None:
        result = GmailParser._parse_date("Tue, 16 Jan 2024 14:00:00 +0000")
        assert result == datetime(2024, 1, 16, 14, 0, 0, tzinfo=UTC)

    def test_empty_string_returns_epoch(self) -> None:
        result = GmailParser._parse_date("")
        assert result == datetime(1970, 1, 1)

    def test_invalid_date_returns_epoch(self) -> None:
        result = GmailParser._parse_date("not-a-real-date")
        assert result == datetime(1970, 1, 1)

    def test_garbage_date_returns_epoch(self) -> None:
        result = GmailParser._parse_date("99/99/9999 99:99:99")
        assert result == datetime(1970, 1, 1)

    def test_none_like_empty_returns_epoch(self) -> None:
        """When date header is missing the parser receives empty string."""
        result = GmailParser._parse_date("")
        assert result == datetime(1970, 1, 1)

    def test_partial_date_returns_epoch(self) -> None:
        result = GmailParser._parse_date("Mon, 15 Jan")
        assert result == datetime(1970, 1, 1)


# ---------------------------------------------------------------------------
# 7. Missing headers handling
# ---------------------------------------------------------------------------


class TestMissingHeaders:
    """Behaviour when expected headers are absent from the payload."""

    def test_missing_subject_defaults(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_subject",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "From", "value": "a@b.com"},
                    {"name": "To", "value": "c@d.com"},
                    {"name": "Date", "value": "Mon, 15 Jan 2024 10:30:00 +0000"},
                ],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.subject == "(no subject)"

    def test_missing_from_defaults(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_from",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "Subject", "value": "Test"},
                    {"name": "To", "value": "c@d.com"},
                    {"name": "Date", "value": "Mon, 15 Jan 2024 10:30:00 +0000"},
                ],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.sender == ""

    def test_missing_to_defaults(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_to",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "Subject", "value": "Test"},
                    {"name": "From", "value": "a@b.com"},
                    {"name": "Date", "value": "Mon, 15 Jan 2024 10:30:00 +0000"},
                ],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.to == ""

    def test_missing_date_returns_epoch(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_date",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "Subject", "value": "Test"},
                    {"name": "From", "value": "a@b.com"},
                    {"name": "To", "value": "c@d.com"},
                ],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.date == datetime(1970, 1, 1)

    def test_no_headers_at_all(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_headers",
            "payload": {
                "mimeType": "text/plain",
                "headers": [],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.subject == "(no subject)"
        assert msg.header.sender == ""
        assert msg.header.to == ""
        assert msg.header.cc == ""
        assert msg.header.message_id_header == ""
        assert msg.header.date == datetime(1970, 1, 1)

    def test_missing_thread_id_defaults_empty(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_thread",
            "payload": {
                "mimeType": "text/plain",
                "headers": [],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.thread_id == ""

    def test_missing_label_ids_defaults_empty_tuple(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_labels",
            "payload": {
                "mimeType": "text/plain",
                "headers": [],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.label_ids == ()

    def test_missing_snippet_defaults_empty(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_snippet",
            "payload": {
                "mimeType": "text/plain",
                "headers": [],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.snippet == ""


# ---------------------------------------------------------------------------
# 8. Empty body handling
# ---------------------------------------------------------------------------


class TestEmptyBodyHandling:
    """When body data is missing or empty."""

    def test_no_body_data_in_payload(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_empty_body",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "Subject", "value": "Empty"},
                    {"name": "From", "value": "a@b.com"},
                    {"name": "To", "value": "c@d.com"},
                    {"name": "Date", "value": "Mon, 15 Jan 2024 10:30:00 +0000"},
                ],
                "body": {},
            },
        }
        msg = parser.parse(raw)
        assert msg.body.plain_text is None
        assert msg.body.html is None

    def test_body_data_is_none(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_null_body",
            "payload": {
                "mimeType": "text/plain",
                "headers": [],
                "body": {"data": None},
            },
        }
        msg = parser.parse(raw)
        assert msg.body.plain_text is None
        assert msg.body.html is None

    def test_multipart_with_empty_parts(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_empty_parts",
            "payload": {
                "mimeType": "multipart/alternative",
                "headers": [],
                "body": {"size": 0},
                "parts": [
                    {"mimeType": "text/plain", "body": {}},
                    {"mimeType": "text/html", "body": {}},
                ],
            },
        }
        msg = parser.parse(raw)
        assert msg.body.plain_text is None
        assert msg.body.html is None

    def test_missing_payload_body_key(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_no_payload_body",
            "payload": {
                "mimeType": "text/plain",
                "headers": [],
            },
        }
        msg = parser.parse(raw)
        assert msg.body.plain_text is None
        assert msg.body.html is None

    def test_empty_payload(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_empty_payload",
            "payload": {},
        }
        msg = parser.parse(raw)
        assert msg.body.plain_text is None
        assert msg.body.html is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """ParseError raised on structurally invalid input."""

    def test_missing_id_raises_parse_error(self, parser: GmailParser) -> None:
        raw: dict[str, Any] = {"payload": {"mimeType": "text/plain", "headers": [], "body": {}}}
        with pytest.raises(ParseError):
            parser.parse(raw)

    def test_non_dict_raises_parse_error(self, parser: GmailParser) -> None:
        with pytest.raises((ParseError, TypeError, AttributeError)):
            parser.parse("not a dict")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Header case-insensitivity
# ---------------------------------------------------------------------------


class TestHeaderCaseInsensitivity:
    """Gmail API headers can arrive in various cases."""

    def test_lowercase_header_names(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_lower_headers",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "subject", "value": "Lower Case"},
                    {"name": "from", "value": "lower@example.com"},
                    {"name": "to", "value": "to@example.com"},
                    {"name": "date", "value": "Mon, 15 Jan 2024 10:30:00 +0000"},
                ],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.subject == "Lower Case"
        assert msg.header.sender == "lower@example.com"

    def test_mixed_case_header_names(self, parser: GmailParser) -> None:
        raw = {
            "id": "msg_mixed_headers",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "SUBJECT", "value": "Upper Case"},
                    {"name": "FROM", "value": "upper@example.com"},
                    {"name": "TO", "value": "to@example.com"},
                    {"name": "DATE", "value": "Mon, 15 Jan 2024 10:30:00 +0000"},
                ],
                "body": {"data": "SGVsbG8="},
            },
        }
        msg = parser.parse(raw)
        assert msg.header.subject == "Upper Case"
        assert msg.header.sender == "upper@example.com"
