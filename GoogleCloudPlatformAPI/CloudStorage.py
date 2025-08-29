"""Helpers for interacting with Google Cloud Storage."""

import glob
import json
import logging
import os
from typing import Any, List, Optional, Union

from google.cloud import storage

from .Oauth import ServiceAccount


class CloudStorage:
    """
    Simple wrapper around the ``google.cloud.storage`` client.

    Attributes
    ----------
    __client : google.cloud.storage.Client
        The Cloud Storage client.
    """

    __client: storage.Client

    def __init__(
        self, credentials: Optional[str] = None, project_id: Optional[str] = None
    ) -> None:
        """
        Initialise the Cloud Storage client.

        Parameters
        ----------
        credentials : str, optional
            Path to a service account JSON file. Defaults to ``None`` which
            relies on environment configuration.
        project_id : str, optional
            Google Cloud project identifier. Defaults to ``None``.
        """
        logging.debug("CloudStorage::__init__")
        if credentials is not None:
            self.__client = storage.Client(credentials=credentials, project=project_id)
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is not None:
            self.__client = storage.Client(
                credentials=ServiceAccount.from_service_account_file(),
                project=project_id,
            )
        else:
            self.__client = storage.Client(project=project_id)

    def __enter__(self) -> "CloudStorage":
        """
        Return the context manager instance.

        Returns
        -------
        CloudStorage
            The Cloud Storage client instance.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Close the underlying client on context exit.

        Parameters
        ----------
        exc_type : Any
            The exception type.
        exc_val : Any
            The exception value.
        exc_tb : Any
            The traceback.
        """
        self.__client.close()

    def list_files(self, bucket_name: str, prefix: str) -> List[str]:
        """
        List blob names under ``prefix`` in ``bucket_name``.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket.
        prefix : str
            The prefix to filter by.

        Returns
        -------
        list[str]
            A list of blob names.
        """
        logging.debug(f"CloudStorage::list_files::{bucket_name}/{prefix}")
        _return: List[str] = []
        blobs = self.__client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            _return.append(blob.name)
        return _return

    def download_as_string(
        self, bucket_name: str, source_blob_name: str, destination_file_name: str
    ) -> None:
        """
        Download a JSON blob and save it locally.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket.
        source_blob_name : str
            The name of the blob to download.
        destination_file_name : str
            The local path to save the file to.
        """
        logging.debug(f"CloudStorage::download_as_string::{destination_file_name}")
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        json_data_string = blob.download_as_string()
        json_data = json.loads(json_data_string)
        with open(destination_file_name, "w") as textfile:
            textfile.write(json.dumps(json_data))

    def upload(
        self,
        bucket_name: str,
        destination_blob_name: str,
        data: Union[str, object],
        override: bool = True,
    ) -> None:
        """
        Upload data to Cloud Storage.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket.
        destination_blob_name : str
            The name of the destination blob.
        data : str or object
            The data to upload.
        override : bool, optional
            Whether to override the blob if it already exists. Defaults to ``True``.
        """
        logging.debug("CloudStorage::upload")
        if not override:
            if self.file_exists(
                filepath=destination_blob_name, bucket_name=bucket_name
            ):
                return
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(str(data))

    def upload_from_string(
        self,
        bucket_name: str,
        destination_blob_name: str,
        data: str,
        override: bool = False,
    ) -> None:
        """
        Upload a string to Cloud Storage.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket.
        destination_blob_name : str
            The name of the destination blob.
        data : str
            The string data to upload.
        override : bool, optional
            Whether to override the blob if it already exists. Defaults to ``False``.
        """
        logging.debug("CloudStorage::upload_from_string")
        if not self.file_exists(destination_blob_name, bucket_name) or override:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(data)

    def upload_file_from_filename(
        self,
        local_file_path: str,
        destination_file_path: str,
        bucket_name: str,
        override: bool = False,
    ) -> None:
        """
        Upload a local file to Cloud Storage from a filename.

        Parameters
        ----------
        local_file_path : str
            The path to the local file.
        destination_file_path : str
            The destination path in the bucket.
        bucket_name : str
            The name of the bucket.
        override : bool, optional
            Whether to override the blob if it already exists. Defaults to ``False``.
        """
        logging.debug("CloudStorage::upload_file_from_filename")
        if (
            not self.file_exists(
                filepath=destination_file_path, bucket_name=bucket_name
            )
            or override
        ):
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_file_path)
            blob.upload_from_filename(local_file_path)

    def upload_file(
        self, local_file_path: str, destination_file_path: str, override: bool = False
    ) -> None:
        """
        Upload a file to a ``bucket_name/blob_path`` destination.

        Parameters
        ----------
        local_file_path : str
            The path to the local file.
        destination_file_path : str
            The destination path in the format ``bucket_name/blob_path``.
        override : bool, optional
            Whether to override the blob if it already exists. Defaults to ``False``.
        """
        logging.debug("CloudStorage::upload_file_from_filename")
        file_path = os.path.normpath(destination_file_path)
        path_parts = file_path.split(os.sep)
        bucket_name = path_parts[0]

        blob_path = os.sep.join(path_parts[1:]) if len(path_parts) > 1 else ""

        if (
            not self.file_exists(filepath=blob_path, bucket_name=bucket_name)
            or override
        ):
            bucket_name_obj = self.__client.bucket(bucket_name)
            blob = bucket_name_obj.blob(blob_path)
            blob.upload_from_filename(local_file_path)

    def upload_folder(
        self,
        local_folder: str,
        remote_folder: str,
        bucket_name: str,
        file_mask: str = "*.gz",
        override: bool = False,
    ) -> None:
        """
        Upload all files in a local folder to Cloud Storage.

        Parameters
        ----------
        local_folder : str
            The path to the local folder.
        remote_folder : str
            The destination path in the bucket.
        bucket_name : str
            The name of the bucket.
        file_mask : str, optional
            The file mask to match files in the local folder. Defaults to ``"*.gz"``.
        override : bool, optional
            Whether to override the blobs if they already exist. Defaults to ``False``.
        """
        logging.debug("CloudStorage::upload_folder")
        allfiles = glob.glob(local_folder + file_mask)
        for file in allfiles:
            destination_file_path = remote_folder + os.path.basename(file)
            self.upload_file_from_filename(
                local_file_path=file,
                destination_file_path=destination_file_path,
                bucket_name=bucket_name,
                override=override,
            )

    def file_exists(self, filepath: str, bucket_name: str) -> bool:
        """
        Return ``True`` if ``filepath`` exists in ``bucket_name``.

        Parameters
        ----------
        filepath : str
            The path to the file in the bucket.
        bucket_name : str
            The name of the bucket.

        Returns
        -------
        bool
            ``True`` if the file exists, ``False`` otherwise.
        """
        logging.debug(f"CloudStorage::file_exists::{filepath}")
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(filepath)
        return blob.exists()

    def delete_file(self, filename: str, bucket_name: str) -> None:
        """
        Delete a single blob from Cloud Storage.

        Parameters
        ----------
        filename : str
            The name of the blob to delete.
        bucket_name : str
            The name of the bucket.
        """
        logging.debug("CloudStorage::delete_file")
        source_bucket = self.__client.bucket(bucket_name)
        source_bucket.delete_blob(filename)

    def delete_files(self, bucket_name: str, prefix: str) -> None:
        """
        Delete all blobs with a given prefix.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket.
        prefix : str
            The prefix of the blobs to delete.
        """
        logging.debug("CloudStorage::delete_files")
        blobs = self.__client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            blob.delete()

    def copy_file(
        self,
        bucket_name: str,
        file_name: str,
        destination_bucket_name: str,
        override: bool = False,
    ) -> bool:
        """
        Copy a file between buckets.

        Parameters
        ----------
        bucket_name : str
            The name of the source bucket.
        file_name : str
            The name of the file to copy.
        destination_bucket_name : str
            The name of the destination bucket.
        override : bool, optional
            Whether to override the file if it already exists. Defaults to ``False``.

        Returns
        -------
        bool
            ``True`` if the file was copied, ``False`` otherwise.
        """
        logging.debug("CloudStorage::copy_file")
        if (
            not self.file_exists(
                filepath=file_name, bucket_name=destination_bucket_name
            )
            or override
        ):
            source_bucket = self.__client.bucket(bucket_name)
            source_blob = source_bucket.blob(file_name)
            destination_bucket = self.__client.bucket(destination_bucket_name)

            source_bucket.copy_blob(source_blob, destination_bucket, file_name)
            return True
        return False

    def copy_files(
        self,
        bucket_name: str,
        prefix: str,
        destination_bucket_name: str,
        override: bool = False,
    ) -> None:
        """
        Copy all files with ``prefix`` to another bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the source bucket.
        prefix : str
            The prefix of the files to copy.
        destination_bucket_name : str
            The name of the destination bucket.
        override : bool, optional
            Whether to override the files if they already exist. Defaults to ``False``.
        """
        logging.debug("CloudStorage::copy_files")
        files = self.list_files(bucket_name=bucket_name, prefix=prefix)
        for file in files:
            self.copy_file(
                bucket_name=bucket_name,
                file_name=file,
                destination_bucket_name=destination_bucket_name,
                override=override,
            )
