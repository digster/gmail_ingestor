"""Unit tests for MarkdownConverter."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest

from gmail_ingester.core.converter import MarkdownConverter
from gmail_ingester.core.exceptions import ConversionError
from gmail_ingester.core.models import ConvertedEmail, EmailBody, EmailHeader

MESSAGE_ID = "msg_test_001"


class TestConvertBodyHtml:
    """HTML body conversion via trafilatura."""

    def test_html_converted_to_markdown(
        self, sample_header: EmailHeader, sample_body_html: EmailBody
    ) -> None:
        """Trafilatura extracts text from HTML body."""
        converter = MarkdownConverter()
        with patch("gmail_ingester.core.converter.trafilatura") as mock_traf:
            mock_traf.extract.return_value = "Hello, this is **HTML**."
            result = converter.convert(MESSAGE_ID, sample_header, sample_body_html)

        assert isinstance(result, ConvertedEmail)
        assert result.message_id == MESSAGE_ID
        assert "Hello, this is **HTML**." in result.markdown
        mock_traf.extract.assert_called_once_with(
            sample_body_html.html,
            output_format="txt",
            favor_recall=True,
            include_links=True,
            include_tables=True,
        )

    def test_html_extraction_uses_correct_params(self, sample_header: EmailHeader) -> None:
        """Trafilatura is called with favor_recall, include_links, include_tables."""
        body = EmailBody(
            plain_text=None,
            html="<html><body><p>Content</p></body></html>",
        )
        converter = MarkdownConverter()
        with patch("gmail_ingester.core.converter.trafilatura") as mock_traf:
            mock_traf.extract.return_value = "Content"
            converter.convert(MESSAGE_ID, sample_header, body)

        mock_traf.extract.assert_called_once_with(
            body.html,
            output_format="txt",
            favor_recall=True,
            include_links=True,
            include_tables=True,
        )


class TestFallbackToPlainText:
    """Fallback behaviour when HTML extraction fails or is absent."""

    def test_falls_back_when_trafilatura_returns_none(
        self, sample_header: EmailHeader, sample_body_html: EmailBody
    ) -> None:
        """When trafilatura returns None the converter uses plain_text."""
        converter = MarkdownConverter()
        with patch("gmail_ingester.core.converter.trafilatura") as mock_traf:
            mock_traf.extract.return_value = None
            result = converter.convert(MESSAGE_ID, sample_header, sample_body_html)

        assert sample_body_html.plain_text is not None
        assert sample_body_html.plain_text in result.markdown

    def test_falls_back_when_trafilatura_raises(
        self, sample_header: EmailHeader, sample_body_html: EmailBody
    ) -> None:
        """When trafilatura raises an exception the converter uses plain_text."""
        converter = MarkdownConverter()
        with patch("gmail_ingester.core.converter.trafilatura") as mock_traf:
            mock_traf.extract.side_effect = RuntimeError("parse error")
            result = converter.convert(MESSAGE_ID, sample_header, sample_body_html)

        assert sample_body_html.plain_text is not None
        assert sample_body_html.plain_text in result.markdown

    def test_plain_text_only_body(
        self, sample_header: EmailHeader, sample_body_text_only: EmailBody
    ) -> None:
        """An email with only plain text (no HTML) converts directly."""
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, sample_header, sample_body_text_only)

        assert sample_body_text_only.plain_text is not None
        assert sample_body_text_only.plain_text in result.markdown


class TestConversionError:
    """ConversionError when no content is available."""

    def test_raises_when_both_none(self, sample_header: EmailHeader) -> None:
        """ConversionError is raised when both html and plain_text are None."""
        body = EmailBody(plain_text=None, html=None)
        converter = MarkdownConverter()
        with pytest.raises(ConversionError, match=MESSAGE_ID):
            converter.convert(MESSAGE_ID, sample_header, body)

    def test_raises_when_html_fails_and_no_plain_text(self, sample_header: EmailHeader) -> None:
        """ConversionError raised when trafilatura returns None and no plain_text."""
        body = EmailBody(plain_text=None, html="<html><body>Hi</body></html>")
        converter = MarkdownConverter()
        with patch("gmail_ingester.core.converter.trafilatura") as mock_traf:
            mock_traf.extract.return_value = None
            with pytest.raises(ConversionError, match=MESSAGE_ID):
                converter.convert(MESSAGE_ID, sample_header, body)


class TestYamlFrontMatter:
    """YAML front matter generation from email headers."""

    def test_front_matter_includes_required_fields(
        self, sample_header: EmailHeader, sample_body_text_only: EmailBody
    ) -> None:
        """Front matter contains subject, from, to, and date."""
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, sample_header, sample_body_text_only)

        assert result.markdown.startswith("---\n")
        assert "---" in result.markdown
        assert 'subject: "Test Subject"' in result.markdown
        assert 'from: "sender@example.com"' in result.markdown
        assert 'to: "recipient@example.com"' in result.markdown
        assert "date: 2024-01-15 10:30:00" in result.markdown

    def test_front_matter_includes_cc_when_present(
        self, sample_header: EmailHeader, sample_body_text_only: EmailBody
    ) -> None:
        """Front matter includes cc field when the header has a cc value."""
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, sample_header, sample_body_text_only)

        assert 'cc: "cc@example.com"' in result.markdown

    def test_front_matter_omits_cc_when_empty(self, sample_body_text_only: EmailBody) -> None:
        """Front matter excludes cc line when the header has no cc value."""
        header_no_cc = EmailHeader(
            subject="No CC",
            sender="a@example.com",
            to="b@example.com",
            date=datetime(2024, 6, 1, 12, 0, 0),
            cc="",
            message_id_header="<nocc@example.com>",
        )
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, header_no_cc, sample_body_text_only)

        assert "cc:" not in result.markdown

    def test_front_matter_date_format(self, sample_body_text_only: EmailBody) -> None:
        """Date in front matter uses YYYY-MM-DD HH:MM:SS format."""
        header = EmailHeader(
            subject="Date Test",
            sender="a@example.com",
            to="b@example.com",
            date=datetime(2025, 12, 31, 23, 59, 59),
        )
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, header, sample_body_text_only)

        assert "date: 2025-12-31 23:59:59" in result.markdown


class TestSpecialCharacterEscaping:
    """Special characters in header values are properly escaped."""

    def test_double_quotes_in_subject(self, sample_body_text_only: EmailBody) -> None:
        """Double quotes in subject are escaped in the YAML front matter."""
        header = EmailHeader(
            subject='Re: "Important" meeting',
            sender="a@example.com",
            to="b@example.com",
            date=datetime(2024, 1, 1, 0, 0, 0),
        )
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, header, sample_body_text_only)

        assert 'subject: "Re: \\"Important\\" meeting"' in result.markdown

    def test_double_quotes_in_sender(self, sample_body_text_only: EmailBody) -> None:
        """Double quotes in sender field are escaped."""
        header = EmailHeader(
            subject="Test",
            sender='"John Doe" <john@example.com>',
            to="b@example.com",
            date=datetime(2024, 1, 1, 0, 0, 0),
        )
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, header, sample_body_text_only)

        assert 'from: "\\"John Doe\\" <john@example.com>"' in result.markdown

    def test_double_quotes_in_to(self, sample_body_text_only: EmailBody) -> None:
        """Double quotes in to field are escaped."""
        header = EmailHeader(
            subject="Test",
            sender="a@example.com",
            to='"Jane Doe" <jane@example.com>',
            date=datetime(2024, 1, 1, 0, 0, 0),
        )
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, header, sample_body_text_only)

        assert 'to: "\\"Jane Doe\\" <jane@example.com>"' in result.markdown

    def test_double_quotes_in_cc(self, sample_body_text_only: EmailBody) -> None:
        """Double quotes in cc field are escaped."""
        header = EmailHeader(
            subject="Test",
            sender="a@example.com",
            to="b@example.com",
            date=datetime(2024, 1, 1, 0, 0, 0),
            cc='"Team" <team@example.com>',
        )
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, header, sample_body_text_only)

        assert 'cc: "\\"Team\\" <team@example.com>"' in result.markdown


class TestConvertedEmailResult:
    """Verify the returned ConvertedEmail dataclass."""

    def test_result_header_matches_input(
        self, sample_header: EmailHeader, sample_body_text_only: EmailBody
    ) -> None:
        """The returned ConvertedEmail carries the original header."""
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, sample_header, sample_body_text_only)

        assert result.header is sample_header

    def test_markdown_has_front_matter_and_body(
        self, sample_header: EmailHeader, sample_body_text_only: EmailBody
    ) -> None:
        """Markdown output contains front matter block followed by body content."""
        converter = MarkdownConverter()
        result = converter.convert(MESSAGE_ID, sample_header, sample_body_text_only)

        parts = result.markdown.split("---")
        # parts[0] is empty (before first ---), parts[1] is front matter, parts[2] is body
        assert len(parts) >= 3
        assert sample_body_text_only.plain_text is not None
        assert sample_body_text_only.plain_text in parts[2]
