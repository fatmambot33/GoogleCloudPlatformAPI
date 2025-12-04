"""OAuth helpers for Google APIs.

This module provides helper classes for handling authentication with Google APIs,
supporting both user and service account credentials.

Public Classes
--------------
- ClientCredentials: Handles user or service account credentials based on environment.
- ServiceAccount: Provides helpers specifically for service account authentication.
"""

import logging
import os
from typing import List, Optional, Union

from google.oauth2 import credentials, service_account
from googleads import oauth2
from googleads.oauth2 import GoogleOAuth2Client


class ClientCredentials:
    """
    Handle user or service account credentials.

    This class determines whether to use service account credentials or user
    credentials based on the presence of the
    ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

    Attributes
    ----------
    credentials_path : str or None
        The path to the Google application credentials file.
    """

    def __init__(self) -> None:
        """Initialise with credentials from ``GOOGLE_APPLICATION_CREDENTIALS``."""
        self.credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    @property
    def gcp_credentials(
        self,
    ) -> Union[credentials.Credentials, service_account.Credentials]:
        """
        Return Google Cloud credentials.

        Returns
        -------
        google.oauth2.credentials.Credentials or google.oauth2.service_account.Credentials
            The generated credentials object.

        Raises
        ------
        google.auth.exceptions.DefaultCredentialsError
            If no credentials are found in the environment.
        """
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        if self.credentials_path is not None:
            logging.debug("ClientCredentials::gcp_credentials::service_account")
            return service_account.Credentials.from_service_account_file(
                filename=self.credentials_path, scopes=scopes
            )
        logging.debug("ClientCredentials::gcp_credentials::user_account")
        return credentials.Credentials(scopes=scopes)  # type: ignore

    @property
    def get_service_account_client(
        self,
    ) -> Union[oauth2.GoogleServiceAccountClient, GoogleOAuth2Client]:
        """
        Return a Google Ads service account client.

        Returns
        -------
        googleads.oauth2.GoogleServiceAccountClient or googleads.oauth2.GoogleOAuth2Client
            The Google Ads client.

        Raises
        ------
        google.auth.exceptions.DefaultCredentialsError
            If no credentials are found in the environment.
        """
        scope = oauth2.GetAPIScope("ad_manager")
        if self.credentials_path is not None:
            logging.debug(
                "ClientCredentials::get_service_account_client::service_account"
            )
            return oauth2.GoogleServiceAccountClient(
                key_file=self.credentials_path, scope=scope
            )
        logging.debug("ClientCredentials::get_service_account_client::user_account")
        return oauth2.GoogleOAuth2Client()

    def get_cloudplatform(
        self,
        credentials_path: Optional[str] = None,
        scopes: Optional[List[str]] = [
            "https://www.googleapis.com/auth/cloud-platform"
        ],
    ) -> Union[credentials.Credentials, service_account.Credentials]:
        """
        Return Google Cloud credentials for the given scopes.

        Parameters
        ----------
        credentials_path : str, optional
            Path to a service account JSON file. If ``None``, user credentials
            are used. Defaults to ``None``.
        scopes : list[str], optional
            The scopes to request. Defaults to ``["https://www.googleapis.com/auth/cloud-platform"]``.

        Returns
        -------
        google.oauth2.credentials.Credentials or google.oauth2.service_account.Credentials
            The generated credentials object.

        Raises
        ------
        google.auth.exceptions.DefaultCredentialsError
            If no credentials are found in the environment.
        """
        if credentials_path is not None:
            logging.debug("ClientCredentials::get_cloudplatform::service_account")
            return service_account.Credentials.from_service_account_file(
                filename=credentials_path, scopes=scopes
            )
        logging.debug("ClientCredentials::get_cloudplatform::user_account")
        return credentials.Credentials(scopes=scopes)  # type: ignore


class ServiceAccount:
    """Helpers for service account authentication."""

    @staticmethod
    def from_service_account_file(
        credentials: Optional[str] = None,
        scopes: Optional[List[str]] = [
            "https://www.googleapis.com/auth/cloud-platform"
        ],
    ) -> service_account.Credentials:
        """
        Create credentials from a service account file.

        Parameters
        ----------
        credentials : str, optional
            Path to a service account JSON file. If ``None``, the value from
            ``GOOGLE_APPLICATION_CREDENTIALS`` is used.
        scopes : list[str], optional
            The scopes to request. Defaults to ``["https://www.googleapis.com/auth/cloud-platform"]``.

        Returns
        -------
        google.oauth2.service_account.Credentials
            The generated credentials object.

        Raises
        ------
        google.auth.exceptions.DefaultCredentialsError
            If credentials are not provided and the environment variable is not set.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Oauth import ServiceAccount

        # Assumes GOOGLE_APPLICATION_CREDENTIALS is set
        creds = ServiceAccount.from_service_account_file()
        ```
        """
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return service_account.Credentials.from_service_account_file(
            filename=credentials, scopes=scopes
        )

    @staticmethod
    def get_service_account_client(
        credentials: Optional[str] = None, scope: Optional[str] = "ad_manager"
    ) -> oauth2.GoogleServiceAccountClient:
        """
        Return a Google Ads service account client.

        Parameters
        ----------
        credentials : str, optional
            Path to a service account JSON file. If ``None``, the value from
            ``GOOGLE_APPLICATION_CREDENTIALS`` is used.
        scope : str, optional
            The API scope to request. Defaults to ``"ad_manager"``.

        Returns
        -------
        googleads.oauth2.GoogleServiceAccountClient
            The Google Ads service account client.

        Raises
        ------
        google.auth.exceptions.DefaultCredentialsError
            If credentials are not provided and the environment variable is not set.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.Oauth import ServiceAccount

        # Assumes GOOGLE_APPLICATION_CREDENTIALS is set
        ads_client = ServiceAccount.get_service_account_client()
        ```
        """
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return oauth2.GoogleServiceAccountClient(
            key_file=credentials, scope=oauth2.GetAPIScope(scope)
        )
