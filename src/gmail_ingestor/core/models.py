"""Frozen dataclasses for the Gmail Ingestor domain model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class MessageStub:
    """Lightweight message reference from Gmail list API."""

    message_id: str
    thread_id: str


@dataclass(frozen=True)
class EmailHeader:
    """Parsed email headers."""

    subject: str
    sender: str
    to: str
    date: datetime
    cc: str = ""
    message_id_header: str = ""


@dataclass(frozen=True)
class EmailBody:
    """Parsed email body content. At least one of plain_text or html will be set."""

    plain_text: str | None = None
    html: str | None = None


@dataclass(frozen=True)
class EmailMessage:
    """Complete parsed email with headers and body."""

    message_id: str
    thread_id: str
    label_ids: tuple[str, ...] = field(default_factory=tuple)
    header: EmailHeader | None = None
    body: EmailBody | None = None
    snippet: str = ""


@dataclass(frozen=True)
class ConvertedEmail:
    """Result of converting an email to markdown."""

    message_id: str
    markdown: str
    header: EmailHeader


@dataclass
class FetchProgress:
    """Mutable progress tracker for pipeline status reporting."""

    total_estimated: int = 0
    ids_discovered: int = 0
    messages_fetched: int = 0
    messages_converted: int = 0
    messages_failed: int = 0
    current_stage: str = "idle"
