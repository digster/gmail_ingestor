"""Shared fixtures for Gmail Ingestor tests."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from gmail_ingestor.core.models import EmailBody, EmailHeader, EmailMessage

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the test fixtures directory."""
    return FIXTURES_DIR


@pytest.fixture
def simple_text_raw() -> dict[str, Any]:
    """Raw Gmail API response for a simple text email."""
    return json.loads((FIXTURES_DIR / "simple_text.json").read_text())


@pytest.fixture
def simple_html_raw() -> dict[str, Any]:
    """Raw Gmail API response for a simple HTML email."""
    return json.loads((FIXTURES_DIR / "simple_html.json").read_text())


@pytest.fixture
def multipart_alt_raw() -> dict[str, Any]:
    """Raw Gmail API response for a multipart/alternative email."""
    return json.loads((FIXTURES_DIR / "multipart_alternative.json").read_text())


@pytest.fixture
def multipart_mixed_raw() -> dict[str, Any]:
    """Raw Gmail API response for a multipart/mixed email with attachment."""
    return json.loads((FIXTURES_DIR / "multipart_mixed.json").read_text())


@pytest.fixture
def sample_header() -> EmailHeader:
    """A sample parsed email header."""
    return EmailHeader(
        subject="Test Subject",
        sender="sender@example.com",
        to="recipient@example.com",
        date=datetime(2024, 1, 15, 10, 30, 0),
        cc="cc@example.com",
        message_id_header="<test@example.com>",
    )


@pytest.fixture
def sample_body_html() -> EmailBody:
    """A sample email body with both text and HTML."""
    return EmailBody(
        plain_text="Hello, this is plain text.",
        html="<html><body><p>Hello, this is <b>HTML</b>.</p></body></html>",
    )


@pytest.fixture
def sample_body_text_only() -> EmailBody:
    """A sample email body with only plain text."""
    return EmailBody(plain_text="Hello, plain text only.", html=None)


@pytest.fixture
def sample_email(sample_header: EmailHeader, sample_body_html: EmailBody) -> EmailMessage:
    """A sample complete parsed email."""
    return EmailMessage(
        message_id="msg_test_001",
        thread_id="thread_test_001",
        label_ids=("INBOX",),
        header=sample_header,
        body=sample_body_html,
        snippet="Hello, this is plain text.",
    )


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary output directory for tests."""
    output = tmp_path / "output"
    output.mkdir()
    return output


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Temporary database path for tests."""
    return tmp_path / "test.db"
