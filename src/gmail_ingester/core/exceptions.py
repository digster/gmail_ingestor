"""Custom exceptions for the Gmail Ingester."""


class GmailIngesterError(Exception):
    """Base exception for all Gmail Ingester errors."""


class AuthenticationError(GmailIngesterError):
    """Failed to authenticate with Gmail API."""


class RateLimitError(GmailIngesterError):
    """Gmail API rate limit exceeded."""


class ParseError(GmailIngesterError):
    """Failed to parse email MIME content."""


class ConversionError(GmailIngesterError):
    """Failed to convert email content to markdown."""
