"""OAuth 2.0 authentication with token caching for Gmail API."""

from __future__ import annotations

import logging
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build

from gmail_ingester.core.exceptions import AuthenticationError

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def authenticate(credentials_path: Path, token_path: Path) -> Credentials:
    """Authenticate with Gmail API, using cached token if available.

    Args:
        credentials_path: Path to OAuth 2.0 client credentials JSON.
        token_path: Path to store/load the OAuth token.

    Returns:
        Valid Google OAuth2 credentials.

    Raises:
        AuthenticationError: If authentication fails.
    """
    creds: Credentials | None = None

    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        except Exception as e:
            logger.warning("Failed to load cached token: %s", e)
            creds = None

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_token(creds, token_path)
            return creds
        except Exception as e:
            logger.warning("Token refresh failed, re-authenticating: %s", e)

    if not credentials_path.exists():
        raise AuthenticationError(
            f"Credentials file not found: {credentials_path}. "
            "Download it from Google Cloud Console."
        )

    try:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        creds = flow.run_local_server(port=0)
        _save_token(creds, token_path)
        logger.info("Authentication successful, token cached at %s", token_path)
        return creds
    except Exception as e:
        raise AuthenticationError(f"OAuth flow failed: {e}") from e


def build_gmail_service(creds: Credentials) -> Resource:
    """Build a Gmail API service resource.

    Args:
        creds: Valid Google OAuth2 credentials.

    Returns:
        Gmail API service resource.
    """
    return build("gmail", "v1", credentials=creds)


def _save_token(creds: Credentials, token_path: Path) -> None:
    """Save credentials to the token cache file."""
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())
