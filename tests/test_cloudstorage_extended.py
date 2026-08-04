"""Additional deterministic tests for Cloud Storage boundaries."""

from unittest.mock import MagicMock, patch

from GoogleCloudPlatformAPI.CloudStorage import CloudStorage


def _storage_with_client():
    """Return a CloudStorage instance backed by a mock client."""
    client = MagicMock()
    with patch("GoogleCloudPlatformAPI.CloudStorage.storage.Client", return_value=client):
        storage = CloudStorage(project_id="demo")
    return storage, client


def test_init_uses_explicit_credentials():
    credentials = MagicMock()

    with patch("GoogleCloudPlatformAPI.CloudStorage.storage.Client") as client_class:
        CloudStorage(credentials=credentials, project_id="demo")

    client_class.assert_called_once_with(credentials=credentials, project="demo")


def test_init_uses_service_account_from_environment(monkeypatch):
    credentials = MagicMock()
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/key.json")

    with patch(
        "GoogleCloudPlatformAPI.CloudStorage.ServiceAccount.from_service_account_file",
        return_value=credentials,
    ) as loader, patch(
        "GoogleCloudPlatformAPI.CloudStorage.storage.Client"
    ) as client_class:
        CloudStorage(project_id="demo")

    loader.assert_called_once_with()
    client_class.assert_called_once_with(credentials=credentials, project="demo")


def test_context_manager_closes_client():
    storage, client = _storage_with_client()

    with storage as entered:
        assert entered is storage

    client.close.assert_called_once_with()


def test_list_files_returns_blob_names():
    storage, client = _storage_with_client()
    client.list_blobs.return_value = [
        MagicMock(name="first", **{"name": "folder/a.json"}),
        MagicMock(name="second", **{"name": "folder/b.json"}),
    ]

    assert storage.list_files("bucket", "folder/") == [
        "folder/a.json",
        "folder/b.json",
    ]


def test_upload_from_string_respects_override():
    storage, client = _storage_with_client()
    bucket = client.bucket.return_value
    blob = bucket.blob.return_value

    with patch.object(storage, "file_exists", return_value=True):
        storage.upload_from_string("bucket", "item.txt", "value")
        blob.upload_from_string.assert_not_called()

        storage.upload_from_string("bucket", "item.txt", "value", override=True)

    blob.upload_from_string.assert_called_once_with("value")


def test_upload_file_from_filename_respects_override():
    storage, client = _storage_with_client()
    blob = client.bucket.return_value.blob.return_value

    with patch.object(storage, "file_exists", return_value=False):
        storage.upload_file_from_filename(
            "local.json", "remote/item.json", "bucket"
        )

    blob.upload_from_filename.assert_called_once_with("local.json")


def test_upload_file_splits_bucket_and_blob_path():
    storage, client = _storage_with_client()
    blob = client.bucket.return_value.blob.return_value

    with patch.object(storage, "file_exists", return_value=False) as exists:
        storage.upload_file("local.json", "bucket/folder/item.json")

    exists.assert_called_once_with(filepath="folder/item.json", bucket_name="bucket")
    client.bucket.assert_called_once_with("bucket")
    client.bucket.return_value.blob.assert_called_once_with("folder/item.json")
    blob.upload_from_filename.assert_called_once_with("local.json")


def test_upload_folder_dispatches_matching_files():
    storage, _ = _storage_with_client()

    with patch(
        "GoogleCloudPlatformAPI.CloudStorage.glob.glob",
        return_value=["/tmp/a.gz", "/tmp/b.gz"],
    ), patch.object(storage, "upload_file_from_filename") as upload:
        storage.upload_folder("/tmp/", "remote/", "bucket", override=True)

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
    storage, _ = _storage_with_client()

    with patch.object(
        storage, "list_files", return_value=["folder/a", "folder/b"]
    ) as listed, patch.object(storage, "copy_file") as copied:
        storage.copy_files("source", "folder/", "destination", override=True)

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
