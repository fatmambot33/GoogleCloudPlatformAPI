import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.CloudStorage import CloudStorage


from google.auth.credentials import Credentials


class TestCloudStorage(unittest.TestCase):
    @patch("google.auth.default")
    @patch("google.cloud.storage.Client")
    def test_list_files(self, mock_storage_client, mock_auth_default):
        """Test the list_files method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client
        cs = CloudStorage()
        cs._CloudStorage__client = mock_client
        cs.list_files("my-bucket", "my-prefix")
        mock_client.list_blobs.assert_called_once_with("my-bucket", prefix="my-prefix")

    @patch("google.auth.default")
    @patch("google.cloud.storage.Client")
    def test_file_exists_true(self, mock_storage_client, mock_auth_default):
        """Test the file_exists method when the file exists."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        cs = CloudStorage()
        cs._CloudStorage__client = mock_client
        self.assertTrue(cs.file_exists("my-file", "my-bucket"))
        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("my-file")

    @patch("google.auth.default")
    @patch("google.cloud.storage.Client")
    def test_file_exists_false(self, mock_storage_client, mock_auth_default):
        """Test the file_exists method when the file does not exist."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client
        cs = CloudStorage()
        cs._CloudStorage__client = mock_client
        self.assertFalse(cs.file_exists("my-file", "my-bucket"))
        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("my-file")


if __name__ == "__main__":
    unittest.main()
