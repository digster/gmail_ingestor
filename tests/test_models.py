"""Unit tests for gmail_ingester.core.models dataclasses."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime

import pytest

from gmail_ingester.core.models import (
    ConvertedEmail,
    EmailBody,
    EmailHeader,
    EmailMessage,
    FetchProgress,
    MessageStub,
)

# ---------------------------------------------------------------------------
# MessageStub
# ---------------------------------------------------------------------------


class TestMessageStub:
    """MessageStub is a frozen dataclass with message_id and thread_id."""

    def test_stores_fields(self) -> None:
        stub = MessageStub(message_id="msg_1", thread_id="thread_1")
        assert stub.message_id == "msg_1"
        assert stub.thread_id == "thread_1"

    def test_frozen(self) -> None:
        stub = MessageStub(message_id="msg_1", thread_id="thread_1")
        with pytest.raises(FrozenInstanceError):
            stub.message_id = "msg_2"  # type: ignore[misc]

    def test_equality(self) -> None:
        a = MessageStub(message_id="m", thread_id="t")
        b = MessageStub(message_id="m", thread_id="t")
        assert a == b

    def test_inequality(self) -> None:
        a = MessageStub(message_id="m1", thread_id="t")
        b = MessageStub(message_id="m2", thread_id="t")
        assert a != b


# ---------------------------------------------------------------------------
# EmailHeader
# ---------------------------------------------------------------------------


class TestEmailHeader:
    """EmailHeader stores all header fields with defaults for cc and message_id_header."""

    @pytest.fixture()
    def header_date(self) -> datetime:
        return datetime(2025, 6, 15, 9, 30, 0)

    def test_stores_all_fields(self, header_date: datetime) -> None:
        header = EmailHeader(
            subject="Hello",
            sender="alice@example.com",
            to="bob@example.com",
            date=header_date,
            cc="carol@example.com",
            message_id_header="<abc@example.com>",
        )
        assert header.subject == "Hello"
        assert header.sender == "alice@example.com"
        assert header.to == "bob@example.com"
        assert header.date == header_date
        assert header.cc == "carol@example.com"
        assert header.message_id_header == "<abc@example.com>"

    def test_defaults(self, header_date: datetime) -> None:
        header = EmailHeader(
            subject="Hi",
            sender="a@b.com",
            to="c@d.com",
            date=header_date,
        )
        assert header.cc == ""
        assert header.message_id_header == ""

    def test_frozen(self, header_date: datetime) -> None:
        header = EmailHeader(
            subject="Hi",
            sender="a@b.com",
            to="c@d.com",
            date=header_date,
        )
        with pytest.raises(FrozenInstanceError):
            header.subject = "Changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# EmailBody
# ---------------------------------------------------------------------------


class TestEmailBody:
    """EmailBody stores optional plain_text and html, both defaulting to None."""

    def test_defaults_to_none(self) -> None:
        body = EmailBody()
        assert body.plain_text is None
        assert body.html is None

    def test_plain_text_only(self) -> None:
        body = EmailBody(plain_text="Hello")
        assert body.plain_text == "Hello"
        assert body.html is None

    def test_html_only(self) -> None:
        body = EmailBody(html="<p>Hello</p>")
        assert body.plain_text is None
        assert body.html == "<p>Hello</p>"

    def test_both_parts(self) -> None:
        body = EmailBody(plain_text="Hello", html="<p>Hello</p>")
        assert body.plain_text == "Hello"
        assert body.html == "<p>Hello</p>"

    def test_frozen(self) -> None:
        body = EmailBody(plain_text="Hello")
        with pytest.raises(FrozenInstanceError):
            body.plain_text = "Changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# EmailMessage
# ---------------------------------------------------------------------------


class TestEmailMessage:
    """EmailMessage stores all fields with proper defaults."""

    def test_required_fields_only(self) -> None:
        msg = EmailMessage(message_id="m1", thread_id="t1")
        assert msg.message_id == "m1"
        assert msg.thread_id == "t1"
        assert msg.label_ids == ()
        assert msg.header is None
        assert msg.body is None
        assert msg.snippet == ""

    def test_all_fields(self) -> None:
        dt = datetime(2025, 1, 1, 12, 0, 0)
        header = EmailHeader(
            subject="Subj",
            sender="s@e.com",
            to="t@e.com",
            date=dt,
        )
        body = EmailBody(plain_text="text")
        msg = EmailMessage(
            message_id="m1",
            thread_id="t1",
            label_ids=("INBOX", "UNREAD"),
            header=header,
            body=body,
            snippet="text",
        )
        assert msg.label_ids == ("INBOX", "UNREAD")
        assert msg.header is header
        assert msg.body is body
        assert msg.snippet == "text"

    def test_label_ids_default_is_empty_tuple(self) -> None:
        msg = EmailMessage(message_id="m1", thread_id="t1")
        assert msg.label_ids == ()
        assert isinstance(msg.label_ids, tuple)

    def test_frozen(self) -> None:
        msg = EmailMessage(message_id="m1", thread_id="t1")
        with pytest.raises(FrozenInstanceError):
            msg.message_id = "m2"  # type: ignore[misc]

    def test_label_ids_default_not_shared(self) -> None:
        """Each instance gets its own default tuple (immutable anyway, but verify)."""
        a = EmailMessage(message_id="a", thread_id="t")
        b = EmailMessage(message_id="b", thread_id="t")
        assert a.label_ids == b.label_ids


# ---------------------------------------------------------------------------
# ConvertedEmail
# ---------------------------------------------------------------------------


class TestConvertedEmail:
    """ConvertedEmail is frozen and stores message_id, markdown, and header."""

    @pytest.fixture()
    def header(self) -> EmailHeader:
        return EmailHeader(
            subject="Test",
            sender="a@b.com",
            to="c@d.com",
            date=datetime(2025, 3, 1, 8, 0, 0),
        )

    def test_stores_fields(self, header: EmailHeader) -> None:
        converted = ConvertedEmail(
            message_id="m1",
            markdown="# Hello",
            header=header,
        )
        assert converted.message_id == "m1"
        assert converted.markdown == "# Hello"
        assert converted.header is header

    def test_frozen(self, header: EmailHeader) -> None:
        converted = ConvertedEmail(
            message_id="m1",
            markdown="# Hello",
            header=header,
        )
        with pytest.raises(FrozenInstanceError):
            converted.markdown = "# Changed"  # type: ignore[misc]

    def test_no_defaults(self) -> None:
        """All three fields are required -- missing any raises TypeError."""
        with pytest.raises(TypeError):
            ConvertedEmail(message_id="m1", markdown="md")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# FetchProgress
# ---------------------------------------------------------------------------


class TestFetchProgress:
    """FetchProgress is mutable and has sensible defaults."""

    def test_defaults(self) -> None:
        progress = FetchProgress()
        assert progress.total_estimated == 0
        assert progress.ids_discovered == 0
        assert progress.messages_fetched == 0
        assert progress.messages_converted == 0
        assert progress.messages_failed == 0
        assert progress.current_stage == "idle"

    def test_mutable(self) -> None:
        progress = FetchProgress()
        progress.total_estimated = 100
        progress.ids_discovered = 50
        progress.messages_fetched = 25
        progress.messages_converted = 20
        progress.messages_failed = 2
        progress.current_stage = "fetching"

        assert progress.total_estimated == 100
        assert progress.ids_discovered == 50
        assert progress.messages_fetched == 25
        assert progress.messages_converted == 20
        assert progress.messages_failed == 2
        assert progress.current_stage == "fetching"

    def test_partial_override(self) -> None:
        progress = FetchProgress(total_estimated=200, current_stage="converting")
        assert progress.total_estimated == 200
        assert progress.current_stage == "converting"
        # Other fields remain at defaults
        assert progress.ids_discovered == 0
        assert progress.messages_fetched == 0

    def test_is_not_frozen(self) -> None:
        """FetchProgress should NOT raise FrozenInstanceError on assignment."""
        progress = FetchProgress()
        progress.current_stage = "done"
        assert progress.current_stage == "done"
