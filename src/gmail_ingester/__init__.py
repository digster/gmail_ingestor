"""Gmail Ingester - Fetch Gmail emails by label and convert to markdown."""

from gmail_ingester.core.models import (
    ConvertedEmail,
    EmailBody,
    EmailHeader,
    EmailMessage,
    FetchProgress,
    MessageStub,
)
from gmail_ingester.pipeline.ingester import EmailIngester

__all__ = [
    "ConvertedEmail",
    "EmailBody",
    "EmailHeader",
    "EmailIngester",
    "EmailMessage",
    "FetchProgress",
    "MessageStub",
]
