import glob
import logging
import os
import json
from typing import List, Optional, Union

from google.cloud import storage

from . import ServiceAccount
from .Utils import ListHelper,FileHelper


class CloudStorage:
    __client: storage.Client

    def __init__(self,
                 credentials: Optional[str] = None,
                 project_id: Optional[str] = None):
        logging.debug(f"CloudStorage::__init__")
        if credentials is not None:
            self.__client = storage.Client(
                credentials=credentials, project=project_id)
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is not None:
            self.__client = storage.Client(
                credentials=ServiceAccount.from_service_account_file(), project=project_id)
        else:
            self.__client = storage.Client(project=project_id)

    def __enter__(self):
        # make a database connection and return it
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.__client.close()

    def list_files(self,
                   bucket_name: str,
                   prefix: str) -> List[str]:

        logging.debug(f"CloudStorage::list_files::{bucket_name}/{prefix}")
        _return = []
        blobs = self.__client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            _return.append(blob.name)
        return _return

    def download_as_string(self, bucket_name: str,
                           source_blob_name: str,
                           destination_file_name: str):
        logging.debug(
            f"CloudStorage::download_as_string::{destination_file_name}")
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        json_data_string = blob.download_as_string()
        json_data = json.loads(json_data_string)
        with open(destination_file_name, "w") as textfile:
            textfile.write(json.dumps(json_data))

    def upload(self,
               bucket_name: str,
               destination_blob_name: str,
               data: Union[str, object],
               override: bool = True):
        logging.debug(f"CloudStorage::upload")
        if not override:
            if self.file_exists(filepath=destination_blob_name, bucket_name=bucket_name):
                return
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data)

    def upload_from_string(
            self,
            bucket_name: str,
            destination_blob_name: str,
            data: str,
            override: bool = False):
        logging.debug(f"CloudStorage::upload_from_string")
        if not self.file_exists(destination_blob_name, bucket_name) or override:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(data)

    def upload_file_from_filename(
            self,
            local_file_path: str,
            destination_file_path: str,
            bucket_name: str,
            override: bool = False):

        logging.debug(f"CloudStorage::upload_file_from_filename")
        if not self.file_exists(filepath=destination_file_path, bucket_name=bucket_name) or override:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_file_path)
            blob.upload_from_filename(local_file_path)

    def upload_file(
            self,
            local_file_path: str,
            destination_file_path: str,
            override: bool = False):

        logging.debug(f"CloudStorage::upload_file_from_filename")
        file_path = os.path.normpath(destination_file_path)
        path_parts = file_path.split(os.sep)
        bucket_name = path_parts[0]

        blob_path = os.sep.join(path_parts[1:]) if len(path_parts) > 1 else ''

        if not self.file_exists(filepath=blob_path,
                                bucket_name=bucket_name) or override:
            bucket_name = self.__client.bucket(bucket_name)
            blob = bucket_name.blob(blob_path)
            blob.upload_from_filename(local_file_path)

    def upload_folder(self, local_folder: str, remote_folder: str, bucket_name: str, file_mask="*.gz", override=False):
        logging.debug(f"CloudStorage::upload_folder")
        allfiles = glob.glob(local_folder + file_mask)
        for file in allfiles:

            destination_file_path = remote_folder+os.path.basename(file)
            local_file_path = file
            bucket = self.__client.bucket(bucket_name)
            if not self.file_exists(filepath=destination_file_path, bucket_name=bucket_name) or override:
                blob = bucket.blob(destination_file_path)
                blob.upload_from_filename(local_file_path)
            self.upload_file_from_filename(
                local_file_path=file, destination_file_path=remote_folder+os.path.basename(file), bucket_name=bucket_name, override=override)

    def file_exists(self, filepath: str, bucket_name: str) -> bool:
        logging.debug(f"CloudStorage::file_exists::{filepath}")
        if self.list_files(bucket_name=bucket_name, prefix=filepath):
            return True
        return False

    def delete_file(
            self, filename: str, bucket_name: str):
        logging.debug(f"CloudStorage::delete_file")
        source_bucket = self.__client.bucket(bucket_name)
        source_bucket.delete_blob(filename)

    def delete_files(self, bucket_name: str, prefix: str):
        logging.debug(f"CloudStorage::delete_files")
        files = self.list_files(bucket_name=bucket_name, prefix=prefix)
        for file in files:
            self.delete_file(bucket_name=bucket_name, filename=file)
        if len(files) == 100:
            self.delete_files(bucket_name=bucket_name, prefix=prefix)

    def copy_file(self,
                  bucket_name: str,
                  file_name: str,
                  destination_bucket_name: str,
                  override: bool = False
                  ) -> bool:
        logging.debug(f"CloudStorage::copy_file")
        if not self.file_exists(filepath=file_name,
                                bucket_name=destination_bucket_name) or override:
            source_bucket = self.__client.bucket(bucket_name)
            source_blob = source_bucket.blob(file_name)
            destination_bucket = self.__client.bucket(destination_bucket_name)

            source_bucket.copy_blob(
                source_blob, destination_bucket, file_name
            )
            return True
        return False

    def copy_files(self,
                   bucket_name: str,
                   prefix: str,
                   destination_bucket_name: str,
                   override: bool = False):
        logging.debug(f"CloudStorage::copy_files")
        files = self.list_files(bucket_name=bucket_name,
                                prefix=prefix)
        for file in files:
            self.copy_file(bucket_name=bucket_name,
                           file_name=file,
                           destination_bucket_name=destination_bucket_name,
                           override=override)
