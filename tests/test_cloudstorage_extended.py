"""Additional deterministic tests for Cloud Storage boundaries."""

from unittest.mock import MagicMock, patch

from GoogleCloudPlatformAPI.CloudStorage import CloudStorage


CLIENT_PATH = "GoogleCloudPlatformAPI.CloudStorage.storage.Client"
SERVICE_ACCOUNT_PATH = (
    "GoogleCloudPlatformAPI.CloudStorage.ServiceAccount.from_service_account_file"
)
GLOB_PATH = "GoogleCloudPlatformAPI.CloudStorage.glob.glob"


def _storage_with_client():
    """Return a CloudStorage instance backed by a mock client."""
    client = MagicMock()
    with patch(CLIENT_PATH, return_value=client):
        cloud_storage = CloudStorage(project_id="demo")
    return cloud_storage, client


def test_init_uses_explicit_credentials():
    credentials = MagicMock()

    with patch(CLIENT_PATH) as client_class:
        CloudStorage(credentials=credentials, project_id="demo")

    client_class.assert_called_once_with(credentials=credentials, project="demo")


def test_init_uses_service_account_from_environment(monkeypatch):
    credentials = MagicMock()
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/key.json")

    with patch(SERVICE_ACCOUNT_PATH, return_value=credentials) as loader:
        with patch(CLIENT_PATH) as client_class:
            CloudStorage(project_id="demo")

    loader.assert_called_once_with()
    client_class.assert_called_once_with(credentials=credentials, project="demo")


def test_context_manager_closes_client():
    cloud_storage, client = _storage_with_client()

    with cloud_storage as entered:
        assert entered is cloud_storage

    client.close.assert_called_once_with()


def test_list_files_returns_blob_names():
    cloud_storage, client = _storage_with_client()
    first = MagicMock()
    second = MagicMock()
    first.name = "folder/a.json"
    second.name = "folder/b.json"
    client.list_blobs.return_value = [first, second]

    result = cloud_storage.list_files("bucket", "folder/")

    assert result == ["folder/a.json", "folder/b.json"]


def test_upload_from_string_respects_override():
    cloud_storage, client = _storage_with_client()
    blob = client.bucket.return_value.blob.return_value

    with patch.object(cloud_storage, "file_exists", return_value=True):
        cloud_storage.upload_from_string("bucket", "item.txt", "value")
        blob.upload_from_string.assert_not_called()
        cloud_storage.upload_from_string(
            "bucket", "item.txt", "value", override=True
        )

    blob.upload_from_string.assert_called_once_with("value")


def test_upload_file_from_filename_respects_override():
    cloud_storage, client = _storage_with_client()
    blob = client.bucket.return_value.blob.return_value

    with patch.object(cloud_storage, "file_exists", return_value=False):
        cloud_storage.upload_file_from_filename(
            "local.json", "remote/item.json", "bucket"
        )

    blob.upload_from_filename.assert_called_once_with("local.json")


def test_upload_file_splits_bucket_and_blob_path():
    cloud_storage, client = _storage_with_client()
    blob = client.bucket.return_value.blob.return_value

    with patch.object(cloud_storage, "file_exists", return_value=False) as exists:
        cloud_storage.upload_file("local.json", "bucket/folder/item.json")

    exists.assert_called_once_with(
        filepath="folder/item.json", bucket_name="bucket"
    )
    client.bucket.assert_called_once_with("bucket")
    client.bucket.return_value.blob.assert_called_once_with("folder/item.json")
    blob.upload_from_filename.assert_called_once_with("local.json")


def test_upload_folder_dispatches_matching_files():
    cloud_storage, _ = _storage_with_client()
    files = ["/tmp/a.gz", "/tmp/b.gz"]

    with patch(GLOB_PATH, return_value=files):
        with patch.object(
            cloud_storage, "upload_file_from_filename"
        ) as upload:
            cloud_storage.upload_folder(
                "/tmp/", "remote/", "bucket", override=True
            )

    assert upload.call_count == 2
    upload.assert_any_call(
        local_file_path="/tmp/a.gz",
        destination_file_path="remote/a.gz",
        bucket_name="bucket",
        override=True,
    )
    upload.assert_any_call(
        local_file_path="/tmp/b.gz",
        destination_file_path="remote/b.gz",
        bucket_name="bucket",
        override=True,
    )


def test_copy_files_dispatches_each_source():
    cloud_storage, _ = _storage_with_client()
    sources = ["folder/a", "folder/b"]

    with patch.object(cloud_storage, "list_files", return_value=sources) as listed:
        with patch.object(cloud_storage, "copy_file") as copied:
            cloud_storage.copy_files(
                "source", "folder/", "destination", override=True
            )

    listed.assert_called_once_with(bucket_name="source", prefix="folder/")
    assert copied.call_count == 2
    copied.assert_any_call(
        bucket_name="source",
        file_name="folder/a",
        destination_bucket_name="destination",
        override=True,
    )
    copied.assert_any_call(
        bucket_name="source",
        file_name="folder/b",
        destination_bucket_name="destination",
        override=True,
    )
