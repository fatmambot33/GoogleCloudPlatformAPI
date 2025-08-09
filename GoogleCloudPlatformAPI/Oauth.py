"""OAuth helpers for Google APIs."""

import logging
from typing import List, Optional
from google.oauth2 import service_account, credentials
from googleads import oauth2
import os


class ClientCredentials:
    """Handle user or service account credentials."""

    def __init__(self) -> None:
        """Initialise with credentials from ``GOOGLE_APPLICATION_CREDENTIALS``."""
        self.credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    @property
    def gcp_credentials(self):
        """Return Google Cloud credentials."""
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        if self.credentials_path is not None:
            logging.debug("gcp_credentials::service_account")
            return service_account.Credentials.from_service_account_file(
                filename=self.credentials_path, scopes=scopes
            )
        logging.debug("gcp_credentials::user_account")
        return credentials.Credentials(scopes=scopes)  # type: ignore

    @property
    def get_service_account_client(self):
        """Return a Google Ads service account client."""
        scope = oauth2.GetAPIScope("ad_manager")
        if self.credentials_path is not None:
            logging.debug("get_service_account_client::service_account")
            return oauth2.GoogleServiceAccountClient(key_file=self.credentials_path, scope=scope)
        logging.debug("get_service_account_client::user_account")
        return oauth2.GoogleOAuth2Client()

    def get_cloudplatform(
        self,
        credentials_path: Optional[str] = None,
        scopes: Optional[List[str]] = ["https://www.googleapis.com/auth/cloud-platform"],
    ):
        """Return Google Cloud credentials for the given scopes."""
        if credentials_path is not None:
            logging.debug("get_cloudplatform::service_account")
            return service_account.Credentials.from_service_account_file(filename=credentials_path, scopes=scopes)
        logging.debug("get_cloudplatform::user_account")
        return credentials.Credentials(scopes=scopes)  # type: ignore


class ServiceAccount:
    """Helpers for service account authentication."""

    @staticmethod
    def from_service_account_file(
        credentials: Optional[str] = None,
        scopes: Optional[List[str]] = ["https://www.googleapis.com/auth/cloud-platform"],
    ):
        """Create credentials from a service account file."""
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return service_account.Credentials.from_service_account_file(filename=credentials, scopes=scopes)

    @staticmethod
    def get_service_account_client(credentials: Optional[str] = None, scope: Optional[str] = "ad_manager"):
        """Return a Google Ads service account client."""
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return oauth2.GoogleServiceAccountClient(key_file=credentials, scope=oauth2.GetAPIScope(scope))

