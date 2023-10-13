import glob
import logging
import os
import json
from typing import List, Optional

from google.cloud import storage

from .ServiceAccount import ServiceAccount


class CloudStorage:
    __client: storage.Client

    def __init__(self,
                 credentials: Optional[str] = None,
                 project_id: Optional[str] = None):
        logging.info(f"CloudStorage::__init__")
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

        logging.info(f"CloudStorage::list_files::{bucket_name}/{prefix}")
        _return = []
        blobs = self.__client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            _return.append(blob.name)
        return _return

    def download_as_string(self, bucket_name: str,
                           source_blob_name: str,
                           destination_file_name: str):
        logging.info(
            f"CloudStorage::download_as_string::{destination_file_name}")
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        json_data_string = blob.download_as_string()
        json_data = json.loads(json_data_string)
        with open(destination_file_name, "w") as textfile:
            textfile.write(json.dumps(json_data))

    def upload_from_string(
            self,
            bucket_name: str,
            destination_blob_name: str,
            data: str,
            override: bool = False):
        if bucket_name is None:
            bucket_name = os.environ.get("DEFAULT_GCS_BUCKET")  # type: ignore
        logging.info(f"CloudStorage::upload_from_string")
        if not self.file_exists(destination_blob_name, bucket_name) or override:
            logging.info("File {} upload start".format(destination_blob_name))
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(data)
            logging.info("File {} upload end".format(destination_blob_name))
        else:
            logging.info("File {} existed".format(destination_blob_name))

    def upload_file_from_filename(
            self,
            local_file_path: str,
            destination_file_path: str,
            bucket_name: str,
            override: bool = False):

        logging.info(f"CloudStorage::upload_file_from_filename")
        if not self.file_exists(filepath=destination_file_path, bucket_name=bucket_name) or override:
            bucket = self.__client.bucket(bucket_name)
            blob = bucket.blob(destination_file_path)
            blob.upload_from_filename(local_file_path)
        else:
            logging.info("File {} existed".format(destination_file_path))

    def upload_folder(self, local_folder: str, remote_folder: str, bucket_name: str, file_mask="*.gz", override=False):
        allfiles = glob.glob(local_folder + file_mask)
        for file in allfiles:
            self.upload_file_from_filename(
                local_file_path=file, destination_file_path=remote_folder+os.path.basename(file), bucket_name=bucket_name, override=override)

    def file_exists(self, filepath: str, bucket_name: str) -> bool:
        logging.info(f"CloudStorage::file_exists::{filepath}")
        if self.list_files(bucket_name=bucket_name, prefix=filepath):
            return True
        return False

    def delete_file(
            self, filename: str, bucket_name: str):
        logging.info(f"CloudStorage::delete_file")
        source_bucket = self.__client.bucket(bucket_name)
        source_bucket.delete_blob(filename)
        logging.info(f"deleted {filename}")

    def delete_files(self, bucket_name: str, prefix: str):
        logging.info(f"CloudStorage::delete_files")
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
        files = self.list_files(bucket_name=bucket_name,
                                prefix=prefix)
        for file in files:
            self.copy_file(bucket_name=bucket_name,
                           file_name=file,
                           destination_bucket_name=destination_bucket_name,
                           override=override)
