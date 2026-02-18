"""Tests for MarkdownWriter — writes converted emails to .md files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from gmail_ingester.core.models import ConvertedEmail, EmailHeader
from gmail_ingester.storage.writer import MarkdownWriter


@pytest.fixture
def sample_converted_email() -> ConvertedEmail:
    """A sample ConvertedEmail for testing the writer."""
    header = EmailHeader(
        subject="Weekly Newsletter",
        sender="news@example.com",
        to="reader@example.com",
        date=datetime(2024, 1, 15, 10, 30, 0),
    )
    return ConvertedEmail(
        message_id="18a3f2b0deadbeef",
        markdown="# Weekly Newsletter\n\nHello, world!",
        header=header,
    )


class TestWrite:
    """write() creates a .md file with the correct name and content."""

    def test_creates_md_file(
        self, tmp_output_dir: Path, sample_converted_email: ConvertedEmail
    ) -> None:
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(sample_converted_email)

        assert path.exists()
        assert path.suffix == ".md"

    def test_filename_follows_pattern(
        self, tmp_output_dir: Path, sample_converted_email: ConvertedEmail
    ) -> None:
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(sample_converted_email)

        # Expected: 2024-01-15_weekly-newsletter_18a3f2b0.md
        assert path.name == "2024-01-15_weekly-newsletter_18a3f2b0.md"

    def test_content_preserved(
        self, tmp_output_dir: Path, sample_converted_email: ConvertedEmail
    ) -> None:
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(sample_converted_email)

        content = path.read_text(encoding="utf-8")
        assert content == "# Weekly Newsletter\n\nHello, world!"

    def test_file_inside_output_dir(
        self, tmp_output_dir: Path, sample_converted_email: ConvertedEmail
    ) -> None:
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(sample_converted_email)

        assert path.parent == tmp_output_dir

    def test_short_id_is_first_8_chars(self, tmp_output_dir: Path) -> None:
        header = EmailHeader(
            subject="Test",
            sender="a@b.com",
            to="c@d.com",
            date=datetime(2025, 6, 1),
        )
        email = ConvertedEmail(
            message_id="abcdef1234567890",
            markdown="content",
            header=header,
        )
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(email)

        assert "abcdef12" in path.name
        assert "abcdef1234567890" not in path.name


class TestFilenameWithSpecialSubjects:
    """Filenames handle special characters and unicode in subjects."""

    def test_special_characters_stripped(self, tmp_output_dir: Path) -> None:
        header = EmailHeader(
            subject="Re: Hello! @#$% World?",
            sender="a@b.com",
            to="c@d.com",
            date=datetime(2024, 3, 20),
        )
        email = ConvertedEmail(
            message_id="sp3c1alchars",
            markdown="body",
            header=header,
        )
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(email)

        assert path.exists()
        # Filename should not contain @, #, $, %, ?, or !
        stem = path.stem  # filename without .md
        assert "@" not in stem
        assert "#" not in stem
        assert "$" not in stem
        assert "?" not in stem

    def test_unicode_subject_normalised(self, tmp_output_dir: Path) -> None:
        header = EmailHeader(
            subject="Caf\u00e9 R\u00e9sum\u00e9 \u00fc\u00f1\u00efc\u00f6d\u00e9",
            sender="a@b.com",
            to="c@d.com",
            date=datetime(2024, 5, 1),
        )
        email = ConvertedEmail(
            message_id="unicode123ab",
            markdown="body",
            header=header,
        )
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(email)

        assert path.exists()
        # All characters in the filename should be ASCII
        assert path.name.isascii()

    def test_empty_subject_becomes_untitled(self, tmp_output_dir: Path) -> None:
        header = EmailHeader(
            subject="",
            sender="a@b.com",
            to="c@d.com",
            date=datetime(2024, 7, 10),
        )
        email = ConvertedEmail(
            message_id="empty_subj01",
            markdown="body",
            header=header,
        )
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(email)

        assert "untitled" in path.name

    def test_only_symbols_subject_becomes_untitled(self, tmp_output_dir: Path) -> None:
        header = EmailHeader(
            subject="!@#$%^&*()",
            sender="a@b.com",
            to="c@d.com",
            date=datetime(2024, 8, 1),
        )
        email = ConvertedEmail(
            message_id="symbols_only",
            markdown="body",
            header=header,
        )
        writer = MarkdownWriter(tmp_output_dir)
        path = writer.write(email)

        assert "untitled" in path.name


class TestSlugify:
    """_slugify handles edge cases for slug generation."""

    def test_basic_slug(self) -> None:
        assert MarkdownWriter._slugify("Hello World") == "hello-world"

    def test_preserves_hyphens(self) -> None:
        assert MarkdownWriter._slugify("hello-world") == "hello-world"

    def test_collapses_whitespace(self) -> None:
        assert MarkdownWriter._slugify("hello   world") == "hello-world"

    def test_strips_leading_trailing_hyphens(self) -> None:
        result = MarkdownWriter._slugify("  --hello--  ")
        assert not result.startswith("-")
        assert not result.endswith("-")

    def test_max_length_truncation(self) -> None:
        long_text = "a" * 100
        result = MarkdownWriter._slugify(long_text, max_length=50)
        assert len(result) <= 50

    def test_custom_max_length(self) -> None:
        result = MarkdownWriter._slugify("hello world foo bar baz", max_length=10)
        assert len(result) <= 10

    def test_empty_string_returns_untitled(self) -> None:
        assert MarkdownWriter._slugify("") == "untitled"

    def test_only_unicode_returns_untitled(self) -> None:
        # Characters that get stripped entirely by NFKD + ascii encoding
        assert MarkdownWriter._slugify("\u4e16\u754c") == "untitled"

    def test_mixed_unicode_and_ascii(self) -> None:
        result = MarkdownWriter._slugify("caf\u00e9 latt\u00e9")
        assert result == "cafe-latte"

    def test_numbers_preserved(self) -> None:
        result = MarkdownWriter._slugify("Issue #42 Fixed")
        assert "42" in result

    def test_underscores_preserved(self) -> None:
        result = MarkdownWriter._slugify("hello_world")
        assert "hello_world" in result


class TestWriterCreatesDirectory:
    """MarkdownWriter constructor creates output directory if needed."""

    def test_creates_nested_directory(self, tmp_path: Path) -> None:
        nested = tmp_path / "deep" / "nested" / "output"
        MarkdownWriter(nested)

        assert nested.exists()
        assert nested.is_dir()
