import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.Oauth import ServiceAccount


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


if __name__ == "__main__":
    unittest.main()
