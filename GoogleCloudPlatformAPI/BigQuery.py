# python
import datetime
import json
import logging
import os
import shutil
from dataclasses import dataclass
from typing import List, Optional, Union
import decimal

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import (QueryJobConfig,
                                   ScalarQueryParameter)
from google.cloud.exceptions import NotFound


from .Oauth import ServiceAccount
from .CloudStorage import CloudStorage

DATA_TYPE_MAPPING = {'object': bigquery.enums.SqlTypeNames.STRING,
                     'int64': bigquery.enums.SqlTypeNames.INT64,
                     'float64': bigquery.enums.SqlTypeNames.FLOAT,
                     'bool': bigquery.enums.SqlTypeNames.BOOL}


class BigQuery():
    __client: bigquery.Client
    SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

    def __init__(self,
                 credentials: Optional[str] = None,
                 project_id: Optional[str] = None):
        logging.debug(f"BigQuery::__init__")
        if credentials is not None:
            self.__client = bigquery.Client(
                credentials=ServiceAccount.from_service_account_file(credentials), project=project_id)
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is not None:
            self.__client = bigquery.Client(
                credentials=ServiceAccount.from_service_account_file(), project=project_id)
        else:
            self.__client = bigquery.Client(project=project_id)

    def __enter__(self):
        # make a database connection and return it
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.__client.close()

    def execute_query(self, query: str,
                      job_config: Optional[bigquery.QueryJobConfig] = None) -> List[bigquery.Row]:
        # logging.debug(query)
        logging.debug(f"BigQuery::execute_query")
        if job_config is not None:
            return [row for row in self.__client.query(query, job_config=job_config).result()]
        else:
            return [row for row in self.__client.query(query).result()]

    @dataclass
    class oSpParam():
        name: str
        value: Union[str, int, float, decimal.Decimal,
                     bool, datetime.datetime, datetime.date]
        type: str

    def execute_stored_procedure(self, sp_name: str, sp_params: List[oSpParam]) -> pd.DataFrame:
        logging.debug(f"BigQuery::execute_sp::{sp_name}")
        sp_instruction_params = "@" + ",@".join(
            [sp_param.name for sp_param in sp_params])

        query = f"CALL `{sp_name}`({sp_instruction_params})"
        query_parameters = []
        for sp_param in sp_params:
            query_parameters.append(
                ScalarQueryParameter(sp_param.name, sp_param.type, sp_param.value))

        job_config = QueryJobConfig(query_parameters=query_parameters)
        query_results = self.execute_query(query, job_config)
        result_list = []
        for result in query_results:
            t = dict(**result)
            result_list.append(t)
        return pd.DataFrame(result_list)

    def table_exists(self, table_id: str) -> bool:
        logging.debug(f"BigQuery::table_exists::{table_id}")
        try:
            self.__client.get_table(table_id)
            return True
        except NotFound:
            return False

    def create_schema_from_table(self, folder: str, dataset: Optional[str] = None) -> Optional[dict]:
        logging.debug(f"BigQuery::create_schema_from_table::{folder}")
        if dataset is None:
            dataset = os.environ.get("DEFAULT_BQ_DATASET")
        schema = {}
        schema['allow_jagged_rows'] = True
        schema['allow_quoted_newlines'] = True
        schema['ignore_unknown_values'] = True
        schema['source_format'] = 'CSV'
        schema['field_delimiter'] = ";"
        schema['skip_leading_rows'] = 1

        schema['table_schema'] = []
        if self.table_exists(f"{dataset}.{folder}"):
            dataset_ref = self.__client.dataset(dataset)  # type: ignore
            table_ref = dataset_ref.table(folder)
            table = self.__client.get_table(table_ref)
            for schema_field in table.schema:
                schema['table_schema'].append({"name": schema_field.name,
                                               'type': schema_field.field_type,
                                               'mode': schema_field.mode})
            cloud_storage = CloudStorage()
            cloud_storage.upload_from_string(
                bucket_name=os.environ.get(
                    "DEFAULT_GCS_BUCKET"),  # type: ignore
                data=json.dumps(
                    schema), destination_blob_name=f"{folder}/schema.json")
            return schema

    def create_external_table(self,
                              dataset_name: str,
                              table_name: str,
                              table_schema: dict,
                              source_uris=List[str],
                              partition_field: str = 'date',
                              time_partioning: bool = False):
        logging.debug(f"BigQuery::create_external_table::{table_name}")
        # Configuring the external data source

        schema = []

        for field in table_schema['table_schema']:
            bq_field = bigquery.SchemaField(name=field['name'],
                                            field_type=field['type'],
                                            mode=field['mode'])
            if field['name'] == 'report_date':
                partition_field = 'report_date'
            if field['name'] == partition_field:
                time_partioning = True
            schema.append(bq_field)

        external_config = bigquery.ExternalConfig(
            source_format=table_schema['source_format'])
        external_config.source_uris = source_uris

        if table_schema['source_format'] == 'CSV':
            options = bigquery.CSVOptions
            options.field_delimiter = table_schema['field_delimiter']
            options.skip_leading_rows = table_schema['skip_leading_rows']

            options.allow_jagged_rows = table_schema['allow_jagged_rows']
            options.allow_quoted_newlines = table_schema['allow_quoted_newlines']

            # Creating the external data source
            bq_dataset = self.__client.dataset(dataset_name)
            bq_table = bigquery.Table(
                bq_dataset.table(table_name), schema=schema)
            if time_partioning:
                bq_table.time_partitioning = bigquery.TimePartitioning(
                    field=partition_field)
            bq_table.external_data_configuration = external_config
            self.__client.create_table(bq_table)
            return True

    def create_table_from_schema(self, folder: str,
                                 dataset: Optional[str] = None,
                                 data_path: Optional[str] = None) -> bool:
        logging.debug(f"BigQuery::create_table_from_schema::{folder}")
        if dataset is None:
            dataset = os.environ.get("DEFAULT_BQ_DATASET")
        if data_path is None:
            data_path = os.environ.get("DATA_PATH")
        if not self.table_exists(f"{dataset}.{folder}"):
            with open(f"{data_path}{folder}/schema.json", mode="r") as schema_file:
                schema_json = json.load(schema_file)

            job_schema = []
            partition_field = 'date'
            for field in schema_json['table_schema']:
                bq_field = bigquery.SchemaField(name=field['name'],
                                                field_type=field['type'],
                                                mode=field['mode'])
                if field['name'] == 'report_date':
                    partition_field = 'report_date'
                job_schema.append(bq_field)
            bq_dataset = self.__client.dataset(dataset)  # type: ignore
            bq_table = bq_dataset.table(folder)
            bq_table = bigquery.Table(bq_table, schema=job_schema)

            bq_table.time_partitioning = bigquery.TimePartitioning(
                field=partition_field)

            self.__client.create_table(bq_table)
            return True
        return False

    def load_from_query(self, query: str,
                        table_id: str,
                        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # type: ignore
                        ):
        logging.debug(f"BigQuery::load_from_query")
        job_config = bigquery.QueryJobConfig(destination=table_id,
                                             allow_large_results=True,
                                             write_disposition=write_disposition)
        query_job = self.__client.query(query=query, job_config=job_config)
        query_job.result()  # Wait for the job to complete.

        logging.debug("Query results loaded to the table {}".format(table_id))

    def delete_partition(self, table_id: str,
                         partition_date: datetime.date,
                         partition_name: str = 'date') -> bool:

        if self.table_exists(table_id):
            logging.debug(
                f"BigQuery::delete_partition::{table_id}::{partition_date.strftime('%Y-%m-%d')}")
            query = (
                "DELETE FROM {} WHERE {} = \'{}\'".format(table_id, partition_name, partition_date.strftime('%Y-%m-%d')))
            query_job = self.__client.query(query)  # API request
            query_job.result()  # Waits for query to finish
            return True
        return False

    def load_from_cloud(self,
                        bucket_name: str,
                        data_set: str,
                        table: str,
                        local_folder: str,
                        remote_folder: str,
                        partition_date: datetime.date,
                        partition_name: str = 'date',
                        file_mask: str = '*.gz',
                        override: bool = False) -> bool:

        table_id = data_set + '.' + table
        logging.debug("BigQuery::load_from_cloud::{}".format(table_id))
        self.delete_partition(table_id, partition_date, partition_name)
        job_config, uri = BigQuery.build_job_config(
            table_name=table_id, bucket_name=bucket_name, partition_date=partition_date, data_path=local_folder)

        load_job = self.__client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()  # Waits for the job to complete.
        return True

    def load_from_local(self,
                        bucket_name: str,
                        data_set: str,
                        table: str,
                        local_folder: str,
                        prefix: str,
                        partition_date: datetime.date,
                        partition_name: str = "date",
                        file_mask: str = "*.csv.gz",
                        override: bool = False) -> bool:

        logging.debug(f'BigQuery::load_from_local::{local_folder}')

        remote_folder = table+"/"
        if partition_date is not None:
            dest_folder = remote_folder + \
                partition_date.strftime('%Y-%m-%d') + '/'
            source_folder = local_folder + \
                partition_date.strftime('%Y-%m-%d') + '/'

        with CloudStorage() as cs:
            if override:
                cs.delete_files(bucket_name=bucket_name, prefix=dest_folder)

            schema_path = local_folder + 'schema.json'

            if not os.path.exists(schema_path):
                cs = CloudStorage()
                cs.download_as_string(bucket_name=bucket_name,
                                      source_blob_name=remote_folder + 'schema.json',
                                      destination_file_name=schema_path)

            partition_schema_path = source_folder + 'schema.json'
            if not os.path.exists(partition_schema_path):
                shutil.copy(schema_path, partition_schema_path)
            cs.upload_folder(local_folder=source_folder,
                             remote_folder=dest_folder,
                             bucket_name=bucket_name,
                             file_mask=file_mask,
                             override=override)
        return self.load_from_cloud(bucket_name=bucket_name,
                                    data_set=data_set,
                                    table=table,
                                    local_folder=local_folder,
                                    remote_folder=remote_folder,
                                    partition_date=partition_date,
                                    partition_name=partition_name,
                                    file_mask=file_mask,
                                    override=override)

    def load_from_uri(self,
                      table_id: str,
                      bucket_name: str,
                      data_path: str,
                      partition_date: datetime.date) -> bool:
        logging.debug('BigQuery::load_from_uri')
        job_config, uri = BigQuery.build_job_config(table_name=table_id,
                                                    partition_date=partition_date,
                                                    bucket_name=bucket_name,
                                                    data_path=data_path)

        self.__client.load_table_from_uri(
            source_uris=uri, destination=table_id, job_config=job_config).result()
        return True

    @staticmethod
    def build_job_config(table_name: str,
                         bucket_name: str,
                         data_path: str,
                         partition_date: datetime.date):
        logging.debug('BigQuery::build_job_config')

        folder_name = data_path
        schema_path = folder_name + 'schema.json'

        if not os.path.exists(schema_path):
            cs = CloudStorage()
            cs.download_as_string(bucket_name=bucket_name,
                                  source_blob_name=folder_name + '/schema.json',
                                  destination_file_name=schema_path)

        partition_schema_path = folder_name + partition_date.strftime(
            '%Y-%m-%d') + '/schema.json'
        if not os.path.exists(partition_schema_path):
            shutil.copy(schema_path, partition_schema_path)

        with open(partition_schema_path, mode="r") as schema_file:
            schema_json = json.load(schema_file)

            job_schema = []
            for field in schema_json['table_schema']:
                bq_field = bigquery.SchemaField(name=field['name'],
                                                field_type=field['type'],
                                                mode=field['mode'])
                job_schema.append(bq_field)
            job_config = bigquery.LoadJobConfig(
                schema=job_schema,
                # max_bad_records=10000,
                allow_jagged_rows=schema_json['allow_jagged_rows'],
                allow_quoted_newlines=schema_json['allow_quoted_newlines'],
                ignore_unknown_values=schema_json['ignore_unknown_values']

            )
            if partition_date is not None:
                uri = "gs://" + bucket_name + '/' + os.path.basename(os.path.dirname(folder_name)) + "/" + partition_date.strftime(
                    '%Y-%m-%d')
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                uri = "gs://" + bucket_name + '/' + folder_name
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            if schema_json['source_format'] == 'CSV':
                job_config.field_delimiter = schema_json['field_delimiter']
                job_config.skip_leading_rows = schema_json['skip_leading_rows']
                job_config.source_format = bigquery.SourceFormat.CSV
                uri = uri + "/*.csv.gz"
            else:
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                uri = uri + "/*.json.gz"

            return job_config, uri

    @staticmethod
    def sync_from_cloud(
            bucket_name: str,
            data_set: str,
            table: str,
            local_folder: str,
            remote_folder: str,
            partition_date: datetime.date,
            partition_name: str = 'date',
            override: bool = False):

        bq = BigQuery()
        bq.load_from_cloud(bucket_name=bucket_name,
                           data_set=data_set,
                           table=table,
                           local_folder=local_folder,
                           remote_folder=remote_folder,
                           partition_date=partition_date,
                           partition_name=partition_name,
                           override=override)

    @staticmethod
    def sync_from_local(bucket_name: str,
                        data_set: str,
                        table: str,
                        local_folder: str,
                        prefix: str,
                        partition_date: datetime.date,
                        partition_name: str = "date",
                        file_mask: str = "*.csv.gz",
                        override: bool = False):

        bq = BigQuery()
        bq.load_from_local(bucket_name=bucket_name,
                           data_set=data_set,
                           table=table,
                           local_folder=local_folder,
                           prefix=prefix,
                           partition_date=partition_date,
                           partition_name=partition_name,
                           file_mask=file_mask,
                           override=override)

    def bigquery_to_dataframe(self,
                              query_string: str) -> pd.DataFrame:
        logging.debug("bigquery_to_dataframe")
        return self.__client.query(query_string).result().to_dataframe(create_bqstorage_client=True)

    def dataframe_to_bigquery(self,
                              dataframe: pd.DataFrame,
                              table_id: str,
                              write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # type: ignore
                              ):
        """

        :param dataframe: pd.DataFrame: The dataframe to send to BQ
        :param table_id: str: The destination table your-project.your_dataset.your_table_name
        :param write_disposition: bigquery.enums.WriteDisposition: How to handle the table with the data
        """
        # Construct a BigQuery client object.
        bq_schema = []
        df_schema = dict(dataframe.dtypes)

        for item in df_schema.items():
            # Specify the type of columns whose type cannot be auto-detected.
            # For example  pandas dtype "object", so its data type is ambiguous.

            if item[1].name == 'object':
                bq_field = bigquery.SchemaField(item[0],
                                                DATA_TYPE_MAPPING[str(item[1].name)])
                bq_schema.append(bq_field)

        job_config = bigquery.LoadJobConfig(schema=bq_schema,
                                            write_disposition=write_disposition)

        job = self.__client.load_table_from_dataframe(dataframe,
                                                      table_id,
                                                      job_config=job_config)
        job.result()
        table = self.__client.get_table(table_id)
        logging.debug("Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id))
