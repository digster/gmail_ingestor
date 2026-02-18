"""Tests for RawEmailStore — saves original email body content to disk."""

from __future__ import annotations

from pathlib import Path

from gmail_ingester.core.models import EmailBody
from gmail_ingester.storage.raw_store import RawEmailStore


class TestStoreTextAndHtml:
    """store() with both plain_text and html creates two files."""

    def test_creates_two_files(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="Hello plain", html="<p>Hello html</p>")
        result = store.store("msg001", body)

        assert "text" in result
        assert "html" in result
        assert result["text"].exists()
        assert result["html"].exists()

    def test_text_file_named_correctly(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="text", html="<p>html</p>")
        result = store.store("msg001", body)

        assert result["text"].name == "msg001.txt"

    def test_html_file_named_correctly(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="text", html="<p>html</p>")
        result = store.store("msg001", body)

        assert result["html"].name == "msg001.html"

    def test_text_content_preserved(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        plain = "Line one\nLine two\nSpecial chars: <>&\u00e9"
        body = EmailBody(plain_text=plain, html="<p>irrelevant</p>")
        result = store.store("msg_text", body)

        assert result["text"].read_text(encoding="utf-8") == plain

    def test_html_content_preserved(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        html = "<html><body><p>Hello <b>world</b></p></body></html>"
        body = EmailBody(plain_text="irrelevant", html=html)
        result = store.store("msg_html", body)

        assert result["html"].read_text(encoding="utf-8") == html


class TestStoreTextOnly:
    """store() with only plain_text creates one .txt file."""

    def test_creates_only_text_file(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="Only text content", html=None)
        result = store.store("msg_text_only", body)

        assert "text" in result
        assert "html" not in result

    def test_text_file_exists(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="Only text", html=None)
        result = store.store("msg_text_only", body)

        assert result["text"].exists()
        assert result["text"].name == "msg_text_only.txt"

    def test_no_html_file_created(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="text only", html=None)
        store.store("msg_text_only", body)

        html_path = tmp_output_dir / "msg_text_only.html"
        assert not html_path.exists()


class TestStoreHtmlOnly:
    """store() with only html creates one .html file."""

    def test_creates_only_html_file(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text=None, html="<p>HTML only</p>")
        result = store.store("msg_html_only", body)

        assert "html" in result
        assert "text" not in result

    def test_html_file_exists(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text=None, html="<p>html</p>")
        result = store.store("msg_html_only", body)

        assert result["html"].exists()
        assert result["html"].name == "msg_html_only.html"

    def test_no_text_file_created(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text=None, html="<p>html</p>")
        store.store("msg_html_only", body)

        text_path = tmp_output_dir / "msg_html_only.txt"
        assert not text_path.exists()


class TestStoreEmptyBody:
    """store() with both fields as None returns empty dict."""

    def test_returns_empty_dict(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text=None, html=None)
        result = store.store("msg_empty", body)

        assert result == {}

    def test_no_files_created(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text=None, html=None)
        store.store("msg_empty", body)

        files = list(tmp_output_dir.iterdir())
        assert len(files) == 0


class TestStoreReturnsPaths:
    """store() returns a dict mapping 'text'/'html' to Path objects."""

    def test_paths_are_absolute(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="text", html="<p>html</p>")
        result = store.store("msg_abs", body)

        assert result["text"].is_absolute()
        assert result["html"].is_absolute()

    def test_paths_are_inside_raw_dir(self, tmp_output_dir: Path) -> None:
        store = RawEmailStore(tmp_output_dir)
        body = EmailBody(plain_text="text", html="<p>html</p>")
        result = store.store("msg_dir", body)

        assert result["text"].parent == tmp_output_dir
        assert result["html"].parent == tmp_output_dir


class TestStoreCreatesDirectory:
    """RawEmailStore constructor creates the raw directory if it does not exist."""

    def test_creates_nested_directory(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b" / "raw"
        store = RawEmailStore(nested)

        assert nested.exists()
        assert nested.is_dir()

        body = EmailBody(plain_text="hello", html=None)
        result = store.store("msg1", body)
        assert result["text"].exists()
