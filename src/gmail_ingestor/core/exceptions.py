"""Custom exceptions for the Gmail Ingestor."""


class GmailIngestorError(Exception):
    """Base exception for all Gmail Ingestor errors."""


class AuthenticationError(GmailIngestorError):
    """Failed to authenticate with Gmail API."""


class RateLimitError(GmailIngestorError):
    """Gmail API rate limit exceeded."""


class ParseError(GmailIngestorError):
    """Failed to parse email MIME content."""


class ConversionError(GmailIngestorError):
    """Failed to convert email content to markdown."""


class HistoryExpiredError(GmailIngestorError):
    """Gmail history.list returned 404 — the stored historyId has expired."""
