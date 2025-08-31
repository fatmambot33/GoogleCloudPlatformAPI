import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importlib

oauth_mod = importlib.import_module("GoogleCloudPlatformAPI.Oauth")


def test_from_service_account_file():
    with patch("GoogleCloudPlatformAPI.Oauth.service_account") as mock_service_account:
        mock_creds = MagicMock()
        mock_service_account.Credentials.from_service_account_file.return_value = (
            mock_creds
        )

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        result = oauth_mod.ServiceAccount.from_service_account_file(
            credentials="dummy.json", scopes=scopes
        )

        assert result == mock_creds
        mock_service_account.Credentials.from_service_account_file.assert_called_once_with(
            filename="dummy.json", scopes=scopes
        )


@patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "dummy.json"})
def test_gcp_credentials_with_env():
    with patch("GoogleCloudPlatformAPI.Oauth.service_account") as mock_service_account:
        cc = oauth_mod.ClientCredentials()
        _ = cc.gcp_credentials
        mock_service_account.Credentials.from_service_account_file.assert_called_once()


def test_gcp_credentials_without_env():
    with patch("GoogleCloudPlatformAPI.Oauth.credentials") as mock_credentials:
        # Ensure env var not set for this test
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        cc = oauth_mod.ClientCredentials()
        _ = cc.gcp_credentials
        mock_credentials.Credentials.assert_called_once()


@patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "dummy.json"})
def test_get_service_account_client_with_env():
    with patch("GoogleCloudPlatformAPI.Oauth.oauth2") as mock_oauth2:
        cc = oauth_mod.ClientCredentials()
        _ = cc.get_service_account_client
        mock_oauth2.GoogleServiceAccountClient.assert_called_once()


def test_get_service_account_client_without_env():
    with patch("GoogleCloudPlatformAPI.Oauth.oauth2") as mock_oauth2:
        # Ensure env var not set for this test
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        cc = oauth_mod.ClientCredentials()
        _ = cc.get_service_account_client
        mock_oauth2.GoogleOAuth2Client.assert_called_once()
