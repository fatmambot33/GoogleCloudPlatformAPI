import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
import os
from GoogleCloudPlatformAPI.Oauth import ClientCredentials, ServiceAccount


class TestOauth(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.Oauth.service_account")
    def test_from_service_account_file(self, mock_service_account):
        """Test creating credentials from a service account file."""
        mock_creds = MagicMock()
        mock_service_account.Credentials.from_service_account_file.return_value = (
            mock_creds
        )

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        result = ServiceAccount.from_service_account_file(
            credentials="dummy.json", scopes=scopes
        )

        self.assertEqual(result, mock_creds)
        mock_service_account.Credentials.from_service_account_file.assert_called_once_with(
            filename="dummy.json", scopes=scopes
        )

    @patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "dummy.json"})
    @patch("GoogleCloudPlatformAPI.Oauth.service_account")
    def test_gcp_credentials_with_env(self, mock_service_account):
        """Test gcp_credentials with an environment variable."""
        cc = ClientCredentials()
        cc.gcp_credentials
        mock_service_account.Credentials.from_service_account_file.assert_called_once()

    @patch("GoogleCloudPlatformAPI.Oauth.credentials")
    def test_gcp_credentials_without_env(self, mock_credentials):
        """Test gcp_credentials without an environment variable."""
        cc = ClientCredentials()
        cc.gcp_credentials
        mock_credentials.Credentials.assert_called_once()

    @patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "dummy.json"})
    @patch("GoogleCloudPlatformAPI.Oauth.oauth2")
    def test_get_service_account_client_with_env(self, mock_oauth2):
        """Test get_service_account_client with an environment variable."""
        cc = ClientCredentials()
        cc.get_service_account_client
        mock_oauth2.GoogleServiceAccountClient.assert_called_once()

    @patch("GoogleCloudPlatformAPI.Oauth.oauth2")
    def test_get_service_account_client_without_env(self, mock_oauth2):
        """Test get_service_account_client without an environment variable."""
        cc = ClientCredentials()
        cc.get_service_account_client
        mock_oauth2.GoogleOAuth2Client.assert_called_once()


if __name__ == "__main__":
    unittest.main()
