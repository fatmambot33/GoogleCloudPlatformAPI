import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importlib

cs_mod = importlib.import_module("GoogleCloudPlatformAPI.CloudStorage")

from google.auth.credentials import Credentials


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_list_files(mock_storage_client, mock_auth_default):
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"
    mock_auth_default.return_value = (mock_creds, "test-project")
    mock_client = MagicMock()
    mock_storage_client.return_value = mock_client
    cs = cs_mod.CloudStorage()
    cs.list_files("my-bucket", "my-prefix")
    mock_client.list_blobs.assert_called_once_with("my-bucket", prefix="my-prefix")


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_file_exists_true(mock_storage_client, mock_auth_default):
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
    cs = cs_mod.CloudStorage()
    assert cs.file_exists("my-file", "my-bucket") is True
    mock_client.bucket.assert_called_once_with("my-bucket")
    mock_bucket.blob.assert_called_once_with("my-file")


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_file_exists_false(mock_storage_client, mock_auth_default):
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
    cs = cs_mod.CloudStorage()
    assert cs.file_exists("my-file", "my-bucket") is False
    mock_client.bucket.assert_called_once_with("my-bucket")
    mock_bucket.blob.assert_called_once_with("my-file")


@patch("builtins.open", new_callable=mock_open)
@patch("json.dumps")
@patch("json.loads")
@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_download_as_string(
    mock_storage_client,
    mock_auth_default,
    mock_json_loads,
    mock_json_dumps,
    mock_open,
):
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"
    mock_auth_default.return_value = (mock_creds, "test-project")
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.download_as_string.return_value = '{"key": "value"}'
    mock_bucket.blob.return_value = mock_blob
    mock_client.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_client
    cs = cs_mod.CloudStorage()
    cs.download_as_string("my-bucket", "my-file", "my-local-file")
    mock_client.bucket.assert_called_once_with("my-bucket")
    mock_bucket.blob.assert_called_once_with("my-file")
    mock_blob.download_as_string.assert_called_once()
    mock_json_loads.assert_called_once_with('{"key": "value"}')
    mock_open.assert_called()
    mock_json_dumps.assert_called_once()


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_upload(mock_storage_client, mock_auth_default):
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"
    mock_auth_default.return_value = (mock_creds, "test-project")
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_client
    cs = cs_mod.CloudStorage()

    with patch.object(cs, "file_exists", return_value=False):
        cs.upload("my-bucket", "my-file", "my-data")
        mock_blob.upload_from_string.assert_called_once_with("my-data")

    with patch.object(cs, "file_exists", return_value=True):
        cs.upload("my-bucket", "my-file", "my-data", override=False)
        # assert not called again
        mock_blob.upload_from_string.assert_called_once()


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_delete_file(mock_storage_client, mock_auth_default):
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"
    mock_auth_default.return_value = (mock_creds, "test-project")
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_client
    cs = cs_mod.CloudStorage()
    cs.delete_file("my-file", "my-bucket")
    mock_bucket.delete_blob.assert_called_once_with("my-file")


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_delete_files(mock_storage_client, mock_auth_default):
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"
    mock_auth_default.return_value = (mock_creds, "test-project")
    mock_client = MagicMock()
    mock_blob1 = MagicMock()
    mock_blob2 = MagicMock()
    mock_client.list_blobs.return_value = [mock_blob1, mock_blob2]
    mock_storage_client.return_value = mock_client
    cs = cs_mod.CloudStorage()
    cs._CloudStorage__client = mock_client
    cs.delete_files("my-bucket", "my-prefix")
    mock_client.list_blobs.assert_called_once_with("my-bucket", prefix="my-prefix")
    mock_blob1.delete.assert_called_once()
    mock_blob2.delete.assert_called_once()


@patch("google.auth.default")
@patch("google.cloud.storage.Client")
def test_copy_file(mock_storage_client, mock_auth_default):
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"
    mock_auth_default.return_value = (mock_creds, "test-project")
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_client
    cs = cs_mod.CloudStorage()
    cs._CloudStorage__client = mock_client

    with patch.object(cs, "file_exists", return_value=False):
        assert cs.copy_file("my-bucket", "my-file", "my-dest-bucket") is True
        mock_bucket.copy_blob.assert_called_once()

    with patch.object(cs, "file_exists", return_value=True):
        assert (
            cs.copy_file("my-bucket", "my-file", "my-dest-bucket", override=False)
            is False
        )
        # assert not called again
        mock_bucket.copy_blob.assert_called_once()
