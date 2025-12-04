"""Utilities for interacting with Google BigQuery.

This module provides a high-level wrapper for the Google BigQuery API,
simplifying common operations such as executing queries, managing tables,
and loading data from various sources.

Public Classes
--------------
- BigQuery: A helper for common BigQuery operations.
"""

import datetime
import decimal
import json
import logging
import os
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union, Literal

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from google.cloud.exceptions import NotFound
from google import auth

from .CloudStorage import CloudStorage
from .Oauth import ServiceAccount

DATA_TYPE_MAPPING = {
    "object": bigquery.enums.SqlTypeNames.STRING,
    "int64": bigquery.enums.SqlTypeNames.INT64,
    "float64": bigquery.enums.SqlTypeNames.FLOAT,
    "bool": bigquery.enums.SqlTypeNames.BOOL,
}


class BigQuery:
    """
    High-level helper for common BigQuery operations.

    This class provides a simplified interface for interacting with Google
    BigQuery, handling authentication, query execution, data loading, and table
    management.

    Attributes
    ----------
    SCOPES : list[str]
        The scopes required for the BigQuery API.

    Methods
    -------
    execute_query(query, job_config=None)
        Execute a SQL query and return the results.
    execute_stored_procedure(sp_name, sp_params)
        Execute a stored procedure and return its result set.
    table_exists(table_id)
        Check whether a BigQuery table exists.
    create_schema_from_table(folder, dataset=None)
        Create a schema definition file from an existing table.
    create_external_table(...)
        Create an external table from data in Cloud Storage.
    create_table_from_schema(folder, dataset=None, data_path=None)
        Create a table using a local schema file.
    load_from_query(query, table_id, write_disposition=...)
        Execute a query and save the results to a table.
    delete_partition(table_id, partition_date, partition_name='date')
        Delete a specific partition from a table.
    load_from_cloud(...)
        Load data from Cloud Storage into a BigQuery table.
    load_from_local(...)
        Upload local files to Cloud Storage and load into BigQuery.
    load_from_uri(table_id, bucket_name, data_path, partition_date)
        Load data from a Cloud Storage URI into a table.
    build_job_config(table_name, bucket_name, data_path, partition_date)
        Build a BigQuery load job configuration.
    sync_from_cloud(...)
        Synchronise data from Cloud Storage into BigQuery.
    sync_from_local(...)
        Upload local files and synchronise them into BigQuery.
    bigquery_to_dataframe(query_string)
        Execute a query and return the results as a pandas DataFrame.
    dataframe_to_bigquery(dataframe, table_id, write_disposition=...)
        Load a pandas DataFrame into a BigQuery table.
    """

    _client: bigquery.Client
    SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

    def __init__(
        self, credentials: Optional[str] = None, project_id: Optional[str] = None
    ) -> None:
        """
        Initialise the BigQuery client.

        Parameters
        ----------
        credentials : str, optional
            Path to a service account JSON file. Defaults to ``None`` which
            relies on environment configuration.
        project_id : str, optional
            Google Cloud project identifier. Defaults to ``None``.

        Raises
        ------
        google.auth.exceptions.DefaultCredentialsError
            If no credentials are provided and the environment variable is not set.
        """
        logging.debug("BigQuery::__init__")
        if credentials is not None:
            creds = ServiceAccount.from_service_account_file(credentials)
            project_id = creds.project_id
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is not None:
            creds = ServiceAccount.from_service_account_file()
            project_id = creds.project_id
        else:
            creds, project_id = auth.default(scopes=self.SCOPES)
        self._client = bigquery.Client(credentials=creds, project=project_id)  # type: ignore

    def __enter__(self) -> "BigQuery":
        """
        Return context manager instance.

        Returns
        -------
        BigQuery
            The BigQuery client instance.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Close the BigQuery client when leaving the context.

        Parameters
        ----------
        exc_type : Any
            The exception type.
        exc_val : Any
            The exception value.
        exc_tb : Any
            The traceback.
        """
        self._client.close()

    def execute_query(
        self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None
    ) -> List[bigquery.Row]:
        """
        Execute a SQL query and return the resulting rows.

        Parameters
        ----------
        query : str
            SQL query to execute.
        job_config : google.cloud.bigquery.QueryJobConfig, optional
            Optional job configuration used when submitting the query.

        Returns
        -------
        list[google.cloud.bigquery.Row]
            A list of ``Row`` objects representing the query results.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the query fails to execute.

        Examples
        --------
        >>> from GoogleCloudPlatformAPI.BigQuery import BigQuery
        >>> bq = BigQuery()
        >>> rows = bq.execute_query("SELECT 1 AS x")
        >>> print(rows[0]['x'])
        1
        """
        logging.debug("BigQuery::execute_query")
        # Only include job_config when provided to match call expectations in tests.
        if job_config is not None:
            return list(self._client.query(query, job_config=job_config).result())
        return list(self._client.query(query).result())

    @dataclass
    class StoredProcedureParameter:
        """Parameter description for stored procedure execution.

        Attributes
        ----------
        name : str
            The name of the parameter.
        value : str or int or float or decimal.Decimal or bool or datetime.datetime or datetime.date
            The value of the parameter.
        type : str
            The BigQuery type of the parameter.
        """

        name: str
        value: Union[
            str, int, float, decimal.Decimal, bool, datetime.datetime, datetime.date
        ]
        type: str

    def execute_stored_procedure(
        self, sp_name: str, sp_params: List[StoredProcedureParameter]
    ) -> pd.DataFrame:
        """Execute a stored procedure and return its result set.

        Parameters
        ----------
        sp_name : str
            Fully qualified stored procedure name (e.g.,
            ``project.dataset.procedure``).
        sp_params : list[StoredProcedureParameter]
            A list of parameter objects to pass to the stored procedure.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the result set from the stored procedure.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the stored procedure execution fails.
        """
        logging.debug(f"BigQuery::execute_sp::{sp_name}")
        sp_instruction_params = "@" + ",@".join(sp_param.name for sp_param in sp_params)
        query = f"CALL `{sp_name}`({sp_instruction_params})"

        query_parameters = [
            ScalarQueryParameter(sp_param.name, sp_param.type, sp_param.value)
            for sp_param in sp_params
        ]

        job_config = QueryJobConfig(query_parameters=query_parameters)
        query_results = self.execute_query(query, job_config)
        return pd.DataFrame([dict(**result) for result in query_results])

    def table_exists(self, table_id: str) -> bool:
        """Check if a BigQuery table exists.

        Parameters
        ----------
        table_id : str
            Fully qualified table identifier (e.g., ``project.dataset.table``).

        Returns
        -------
        bool
            ``True`` if the table exists, ``False`` otherwise.
        """
        logging.debug(f"BigQuery::table_exists::{table_id}")
        try:
            self._client.get_table(table_id)
            return True
        except NotFound:
            return False

    def create_schema_from_table(
        self, folder: str, dataset: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Create a schema definition file from an existing table.

        This method inspects an existing BigQuery table and generates a JSON
        schema file that can be used to create similar tables. The schema is
        also uploaded to a Cloud Storage bucket.

        Parameters
        ----------
        folder : str
            The name of the table whose schema will be exported.
        dataset : str, optional
            The dataset containing the table. If not provided, the default is
            taken from the ``DEFAULT_BQ_DATASET`` environment variable.

        Returns
        -------
        dict or None
            A dictionary representing the schema if the table exists, otherwise
            ``None``.

        Raises
        ------
        google.cloud.exceptions.NotFound
            If the specified table does not exist.
        """
        logging.debug(f"BigQuery::create_schema_from_table::{folder}")
        if dataset is None:
            dataset = os.environ.get("DEFAULT_BQ_DATASET")
        schema: Dict[str, Any] = {}
        schema["allow_jagged_rows"] = True
        schema["allow_quoted_newlines"] = True
        schema["ignore_unknown_values"] = True
        schema["source_format"] = "CSV"
        schema["field_delimiter"] = ";"
        schema["skip_leading_rows"] = 1

        schema["table_schema"] = []
        if self.table_exists(f"{dataset}.{folder}"):
            dataset_ref = self._client.dataset(dataset)  # type: ignore
            table_ref = dataset_ref.table(folder)
            table = self._client.get_table(table_ref)
            for schema_field in table.schema:
                schema["table_schema"].append(
                    {
                        "name": schema_field.name,
                        "type": schema_field.field_type,
                        "mode": schema_field.mode,
                    }
                )
            cloud_storage = CloudStorage()
            cloud_storage.upload_from_string(
                bucket_name=os.environ.get("DEFAULT_GCS_BUCKET"),  # type: ignore
                data=json.dumps(schema),
                destination_blob_name=f"{folder}/schema.json",
            )
            return schema
        return None

    def create_external_table(
        self,
        dataset_name: str,
        table_name: str,
        table_schema: Dict[str, Any],
        source_uris: List[str],
        partition_field: str = "date",
        time_partioning: bool = False,
    ) -> bool:
        """Create an external table from data in Cloud Storage.

        Parameters
        ----------
        dataset_name : str
            The name of the target dataset.
        table_name : str
            The name of the external table to create.
        table_schema : dict
            A schema definition dictionary, typically generated by
            :meth:`create_schema_from_table`.
        source_uris : list[str]
            A list of Cloud Storage URIs pointing to the external data.
        partition_field : str, optional
            The field to use for time-based partitioning. Default is ``"date"``.
        time_partioning : bool, optional
            Whether to enable time partitioning. Default is ``False``.

        Returns
        -------
        bool
            ``True`` if the table was successfully created.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the table creation API call fails.
        """
        logging.debug(f"BigQuery::create_external_table::{table_name}")

        schema: List[bigquery.SchemaField] = []

        for field in table_schema["table_schema"]:
            bq_field = bigquery.SchemaField(
                name=field["name"], field_type=field["type"], mode=field["mode"]
            )
            if field["name"] == "report_date":
                partition_field = "report_date"
            if field["name"] == partition_field:
                time_partioning = True
            schema.append(bq_field)

        # Map provided source format to the ExternalSourceFormat enum.
        source_format = (
            bigquery.ExternalSourceFormat.CSV
            if str(table_schema.get("source_format", "")).upper() == "CSV"
            else bigquery.ExternalSourceFormat.NEWLINE_DELIMITED_JSON
        )
        external_config = bigquery.ExternalConfig(source_format=source_format)
        external_config.source_uris = source_uris

        if source_format == bigquery.ExternalSourceFormat.CSV:
            csv_options = bigquery.CSVOptions()
            csv_options.field_delimiter = table_schema["field_delimiter"]
            csv_options.skip_leading_rows = table_schema["skip_leading_rows"]
            csv_options.allow_jagged_rows = table_schema["allow_jagged_rows"]
            csv_options.allow_quoted_newlines = table_schema["allow_quoted_newlines"]
            external_config.csv_options = csv_options

            bq_dataset = self._client.dataset(dataset_name)
            bq_table = bigquery.Table(bq_dataset.table(table_name), schema=schema)
            if time_partioning:
                bq_table.time_partitioning = bigquery.TimePartitioning(
                    field=partition_field
                )
            bq_table.external_data_configuration = external_config
            self._client.create_table(bq_table)
            return True
        return False

    def create_table_from_schema(
        self,
        folder: str,
        dataset: Optional[str] = None,
        data_path: Optional[str] = None,
    ) -> bool:
        """Create a BigQuery table from a local schema file.

        Parameters
        ----------
        folder : str
            The folder containing the ``schema.json`` file.
        dataset : str, optional
            The dataset where the table will be created. Defaults to the value
            of the ``DEFAULT_BQ_DATASET`` environment variable.
        data_path : str, optional
            The base path to the schema files. Defaults to the value of the
            ``DATA_PATH`` environment variable.

        Returns
        -------
        bool
            ``True`` if the table was created, ``False`` if it already exists.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the table creation API call fails.
        FileNotFoundError
            If the schema ``.json`` file cannot be found.
        """
        logging.debug(f"BigQuery::create_table_from_schema::{folder}")
        if dataset is None:
            dataset = os.environ.get("DEFAULT_BQ_DATASET")
        if data_path is None:
            data_path = os.environ.get("DATA_PATH")
        if not self.table_exists(f"{dataset}.{folder}"):
            with open(f"{data_path}{folder}/schema.json", mode="r") as schema_file:
                schema_json = json.load(schema_file)

            job_schema: List[bigquery.SchemaField] = []
            partition_field = "date"
            for field in schema_json["table_schema"]:
                bq_field = bigquery.SchemaField(
                    name=field["name"], field_type=field["type"], mode=field["mode"]
                )
                if field["name"] == "report_date":
                    partition_field = "report_date"
                job_schema.append(bq_field)
            bq_dataset = self._client.dataset(dataset)  # type: ignore
            bq_table = bq_dataset.table(folder)
            bq_table = bigquery.Table(bq_table, schema=job_schema)

            bq_table.time_partitioning = bigquery.TimePartitioning(
                field=partition_field
            )

            self._client.create_table(bq_table)
            return True
        return False

    def load_from_query(
        self,
        query: str,
        table_id: str,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,  # type: ignore
    ) -> None:
        """Execute a query and save the results to a destination table.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        table_id : str
            The fully qualified ID of the destination table.
        write_disposition : bigquery.WriteDisposition, optional
            Specifies the action to take if the destination table already
            exists. Defaults to ``WRITE_TRUNCATE``, which overwrites the table.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the query execution or table loading fails.
        """
        logging.debug("BigQuery::load_from_query")
        job_config = bigquery.QueryJobConfig(
            destination=table_id,
            allow_large_results=True,
            write_disposition=write_disposition,
        )
        query_job = self._client.query(query=query, job_config=job_config)
        query_job.result()  # Wait for the job to complete.

        logging.debug(f"Query results loaded to the table {table_id}")

    def delete_partition(
        self, table_id: str, partition_date: datetime.date, partition_name: str = "date"
    ) -> bool:
        """Delete a specific partition from a table.

        Parameters
        ----------
        table_id : str
            The fully qualified ID of the table.
        partition_date : datetime.date
            The date of the partition to be deleted.
        partition_name : str, optional
            The name of the partitioning column. Defaults to ``"date"``.

        Returns
        -------
        bool
            ``True`` if the partition was deleted, ``False`` if the table does
            not exist.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the ``DELETE`` query fails.
        """
        if self.table_exists(table_id):
            logging.debug(
                f"BigQuery::delete_partition::{table_id}::{partition_date.strftime('%Y-%m-%d')}"
            )
            query = f"DELETE FROM {table_id} WHERE {partition_name} = '{partition_date.strftime('%Y-%m-%d')}'"
            query_job = self._client.query(query)  # API request
            query_job.result()  # Waits for query to finish
            return True
        return False

    def load_from_cloud(
        self,
        bucket_name: str,
        data_set: str,
        table: str,
        local_folder: str,
        remote_folder: str,
        partition_date: datetime.date,
        partition_name: str = "date",
        file_mask: str = "*.gz",
        override: bool = False,
    ) -> bool:
        """Load data from Cloud Storage into a BigQuery table.

        Parameters
        ----------
        bucket_name : str
            The name of the source Cloud Storage bucket.
        data_set : str
            The destination BigQuery dataset ID.
        table : str
            The destination BigQuery table ID.
        local_folder : str
            The local folder path for schema files.
        remote_folder : str
            The path in the bucket where the data is stored.
        partition_date : datetime.date
            The partition date to load data into.
        partition_name : str, optional
            The name of the partition column. Defaults to ``"date"``.
        file_mask : str, optional
            The file mask to select files to load. Defaults to ``"*.gz"``.
        override : bool, optional
            If ``True``, the destination partition will be deleted before
            loading. Defaults to ``False``.

        Returns
        -------
        bool
            ``True`` if the load job completes successfully.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the BigQuery load job fails.
        """
        table_id = data_set + "." + table
        logging.debug(f"BigQuery::load_from_cloud::{table_id}")
        if override:
            self.delete_partition(table_id, partition_date, partition_name)
        # Build configuration using the remote folder (GCS path) for URIs and schema.
        job_config, uri = BigQuery.build_job_config(
            table_name=table_id,
            bucket_name=bucket_name,
            partition_date=partition_date,
            data_path=remote_folder,
        )

        load_job = self._client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()  # Waits for the job to complete.
        return True

    def load_from_local(
        self,
        bucket_name: str,
        data_set: str,
        table: str,
        local_folder: str,
        prefix: str,
        partition_date: datetime.date,
        partition_name: str = "date",
        file_mask: str = "*.csv.gz",
        override: bool = False,
    ) -> bool:
        """Upload local files to Cloud Storage and load them into BigQuery.

        Parameters
        ----------
        bucket_name : str
            The destination Cloud Storage bucket.
        data_set : str
            The destination BigQuery dataset.
        table : str
            The destination BigQuery table.
        local_folder : str
            The local directory containing the data files.
        prefix : str
            A prefix for the remote folder path in the bucket.
        partition_date : datetime.date
            The partition date for the data.
        partition_name : str, optional
            The name of the partitioning column. Defaults to ``"date"``.
        file_mask : str, optional
            A file mask to select which files to upload. Defaults to
            ``"*.csv.gz"``.
        override : bool, optional
            If ``True``, existing files in the destination Cloud Storage path
            will be deleted before uploading. Defaults to ``False``.

        Returns
        -------
        bool
            ``True`` if the load job completes successfully.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the Cloud Storage upload or BigQuery load job fails.
        """
        logging.debug(f"BigQuery::load_from_local::{local_folder}")

        remote_folder = table + "/"
        dest_folder = remote_folder
        source_folder = local_folder
        if partition_date is not None:
            dest_folder = remote_folder + partition_date.strftime("%Y-%m-%d") + "/"
            source_folder = local_folder + partition_date.strftime("%Y-%m-%d") + "/"

        with CloudStorage() as cs:
            if override:
                cs.delete_files(bucket_name=bucket_name, prefix=dest_folder)

            schema_path = local_folder + "schema.json"

            if not os.path.exists(schema_path):
                cs = CloudStorage()
                cs.download_as_string(
                    bucket_name=bucket_name,
                    source_blob_name=remote_folder + "schema.json",
                    destination_file_name=schema_path,
                )

            partition_schema_path = source_folder + "schema.json"
            if not os.path.exists(partition_schema_path):
                shutil.copy(schema_path, partition_schema_path)
            cs.upload_folder(
                local_folder=source_folder,
                remote_folder=dest_folder,
                bucket_name=bucket_name,
                file_mask=file_mask,
                override=override,
            )
        return self.load_from_cloud(
            bucket_name=bucket_name,
            data_set=data_set,
            table=table,
            local_folder=local_folder,
            remote_folder=remote_folder,
            partition_date=partition_date,
            partition_name=partition_name,
            file_mask=file_mask,
            override=override,
        )

    def load_from_uri(
        self,
        table_id: str,
        bucket_name: str,
        data_path: str,
        partition_date: datetime.date,
    ) -> bool:
        """Load data from a Cloud Storage URI into a BigQuery table.

        Parameters
        ----------
        table_id : str
            The fully qualified ID of the destination table.
        bucket_name : str
            The name of the Cloud Storage bucket.
        data_path : str
            The path to the data files within the bucket.
        partition_date : datetime.date
            The partition date to load the data into.

        Returns
        -------
        bool
            ``True`` if the load job completes successfully.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the BigQuery load job fails.
        """
        logging.debug("BigQuery::load_from_uri")
        job_config, uri = BigQuery.build_job_config(
            table_name=table_id,
            partition_date=partition_date,
            bucket_name=bucket_name,
            data_path=data_path,
        )

        self._client.load_table_from_uri(
            source_uris=uri, destination=table_id, job_config=job_config
        ).result()
        return True

    @staticmethod
    def build_job_config(
        table_name: str, bucket_name: str, data_path: str, partition_date: datetime.date
    ) -> Tuple[bigquery.LoadJobConfig, str]:
        """Build a BigQuery load job configuration.

        This static method constructs a ``LoadJobConfig`` and a source URI based
        on a local schema file, which is downloaded from Cloud Storage if not
        found locally.

        Parameters
        ----------
        table_name : str
            The name of the target table.
        bucket_name : str
            The name of the Cloud Storage bucket.
        data_path : str
            The path to the data files.
        partition_date : datetime.date
            The partition date for the load job.

        Returns
        -------
        tuple[bigquery.LoadJobConfig, str]
            A tuple containing the configured ``LoadJobConfig`` and the source
            URI for the data.

        Raises
        ------
        FileNotFoundError
            If the schema ``.json`` file is not found locally after attempting to
            download it.
        """
        logging.debug("BigQuery::build_job_config")

        folder_name = data_path
        schema_path = folder_name + "schema.json"

        if not os.path.exists(schema_path):
            cs = CloudStorage()
            cs.download_as_string(
                bucket_name=bucket_name,
                source_blob_name=folder_name + "/schema.json",
                destination_file_name=schema_path,
            )

        partition_schema_path = (
            folder_name + partition_date.strftime("%Y-%m-%d") + "/schema.json"
        )
        if not os.path.exists(partition_schema_path):
            shutil.copy(schema_path, partition_schema_path)

        with open(partition_schema_path, mode="r") as schema_file:
            schema_json = json.load(schema_file)

            job_schema = [
                bigquery.SchemaField(
                    name=field["name"], field_type=field["type"], mode=field["mode"]
                )
                for field in schema_json["table_schema"]
            ]
            job_config = bigquery.LoadJobConfig(
                schema=job_schema,
                # max_bad_records=10000,
                allow_jagged_rows=schema_json["allow_jagged_rows"],
                allow_quoted_newlines=schema_json["allow_quoted_newlines"],
                ignore_unknown_values=schema_json["ignore_unknown_values"],
            )
            if partition_date is not None:
                uri = f"gs://{bucket_name}/{os.path.basename(os.path.dirname(folder_name))}/{partition_date.strftime('%Y-%m-%d')}"
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                uri = f"gs://{bucket_name}/{folder_name}"
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            if schema_json["source_format"] == "CSV":
                job_config.field_delimiter = schema_json["field_delimiter"]
                job_config.skip_leading_rows = schema_json["skip_leading_rows"]
                job_config.source_format = bigquery.SourceFormat.CSV
                uri = f"{uri}/*.csv.gz"
            else:
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                uri = f"{uri}/*.json.gz"

            return job_config, uri

    @staticmethod
    def sync_from_cloud(
        bucket_name: str,
        data_set: str,
        table: str,
        local_folder: str,
        remote_folder: str,
        partition_date: datetime.date,
        partition_name: str = "date",
        override: bool = False,
    ) -> None:
        """Synchronise data from Cloud Storage into a BigQuery table.

        This static method is a convenience wrapper around an instance of the
        :class:`BigQuery` class that calls :meth:`load_from_cloud`.

        Parameters
        ----------
        bucket_name : str
            The name of the source Cloud Storage bucket.
        data_set : str
            The destination BigQuery dataset.
        table : str
            The destination BigQuery table.
        local_folder : str
            The local path for schema files.
        remote_folder : str
            The path in the bucket where data is stored.
        partition_date : datetime.date
            The partition date to synchronise.
        partition_name : str, optional
            The name of the partitioning column. Defaults to ``"date"``.
        override : bool, optional
            If ``True``, the destination partition is deleted before loading.
            Defaults to ``False``.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the BigQuery load job fails.
        """
        bq = BigQuery()
        bq.load_from_cloud(
            bucket_name=bucket_name,
            data_set=data_set,
            table=table,
            local_folder=local_folder,
            remote_folder=remote_folder,
            partition_date=partition_date,
            partition_name=partition_name,
            override=override,
        )

    @staticmethod
    def sync_from_local(
        bucket_name: str,
        data_set: str,
        table: str,
        local_folder: str,
        prefix: str,
        partition_date: datetime.date,
        partition_name: str = "date",
        file_mask: str = "*.csv.gz",
        override: bool = False,
    ) -> None:
        """Upload local files and synchronise them into BigQuery.

        This static method is a convenience wrapper around an instance of the
        :class:`BigQuery` class that calls :meth:`load_from_local`.

        Parameters
        ----------
        bucket_name : str
            The destination Cloud Storage bucket.
        data_set : str
            The destination BigQuery dataset.
        table : str
            The destination BigQuery table.
        local_folder : str
            The local directory containing the data files.
        prefix : str
            A prefix for the remote folder path in the bucket.
        partition_date : datetime.date
            The partition date for the data.
        partition_name : str, optional
            The name of the partitioning column. Defaults to ``"date"``.
        file_mask : str, optional
            A file mask to select which files to upload. Defaults to
            ``"*.csv.gz"``.
        override : bool, optional
            If ``True``, existing files in the destination Cloud Storage path
            will be deleted before uploading. Defaults to ``False``.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the upload or load job fails.
        """
        bq = BigQuery()
        bq.load_from_local(
            bucket_name=bucket_name,
            data_set=data_set,
            table=table,
            local_folder=local_folder,
            prefix=prefix,
            partition_date=partition_date,
            partition_name=partition_name,
            file_mask=file_mask,
            override=override,
        )

    def bigquery_to_dataframe(self, query_string: str) -> pd.DataFrame:
        """Execute a query and return the results as a pandas DataFrame.

        Parameters
        ----------
        query_string : str
            The SQL query to execute.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the query results.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the query fails.

        Examples
        --------
        >>> from GoogleCloudPlatformAPI.BigQuery import BigQuery
        >>> bq = BigQuery()
        >>> df = bq.bigquery_to_dataframe("SELECT 'hello' as greeting")
        >>> print(df['greeting'][0])
        hello
        """
        logging.debug("bigquery_to_dataframe")
        return (
            self._client.query(query_string)
            .result()
            .to_dataframe(create_bqstorage_client=True)
        )

    def dataframe_to_bigquery(
        self,
        dataframe: pd.DataFrame,
        table_id: str,
        write_disposition: Literal[
            "WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"
        ] = "WRITE_TRUNCATE",
    ) -> None:
        """Load a pandas DataFrame into a BigQuery table.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            The DataFrame to be loaded into BigQuery.
        table_id : str
            The fully qualified ID of the destination table.
        write_disposition : str, optional
            Specifies the action to take if the destination table already
            exists. Can be ``"WRITE_TRUNCATE"``, ``"WRITE_APPEND"``, or
            ``"WRITE_EMPTY"``. Defaults to ``"WRITE_TRUNCATE"``.

        Raises
        ------
        google.cloud.exceptions.GoogleCloudError
            If the BigQuery load job fails.

        Examples
        --------
        >>> import pandas as pd
        >>> from GoogleCloudPlatformAPI.BigQuery import BigQuery
        >>> bq = BigQuery()
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> table_id = "your-project.your_dataset.my_table"
        >>> bq.dataframe_to_bigquery(df, table_id)
        """
        # Construct a BigQuery client object.
        bq_schema: List[bigquery.SchemaField] = []
        # Specify the type of columns whose type cannot be auto-detected
        # (e.g. pandas dtype "object" where the data type is ambiguous).
        for col_name, dtype in dataframe.dtypes.items():
            if dtype.name == "object":
                bq_schema.append(
                    bigquery.SchemaField(str(col_name), DATA_TYPE_MAPPING[dtype.name])
                )

        job_config = bigquery.LoadJobConfig(
            schema=bq_schema, write_disposition=write_disposition
        )

        job = self._client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()
        table = self._client.get_table(table_id)
        logging.debug(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
        )
