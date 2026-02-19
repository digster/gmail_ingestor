"""Gmail Ingestor - Fetch Gmail emails by label and convert to markdown."""

from gmail_ingestor.core.models import (
    ConvertedEmail,
    EmailBody,
    EmailHeader,
    EmailMessage,
    FetchProgress,
    MessageStub,
)
from gmail_ingestor.pipeline.ingestor import EmailIngestor

__all__ = [
    "ConvertedEmail",
    "EmailBody",
    "EmailHeader",
    "EmailIngestor",
    "EmailMessage",
    "FetchProgress",
    "MessageStub",
]
