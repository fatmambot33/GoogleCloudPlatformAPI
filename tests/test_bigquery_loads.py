"""Deterministic tests for BigQuery load workflows."""

import datetime
import importlib
import json
from unittest.mock import MagicMock, patch

bqmod = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")


def _bigquery_with_client():
    instance = bqmod.BigQuery.__new__(bqmod.BigQuery)
    instance._client = MagicMock()
    return instance


def _schema(source_format="CSV"):
    return {
        "table_schema": [
            {"name": "date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "value", "type": "STRING", "mode": "NULLABLE"},
        ],
        "allow_jagged_rows": True,
        "allow_quoted_newlines": True,
        "ignore_unknown_values": False,
        "source_format": source_format,
        "field_delimiter": ",",
        "skip_leading_rows": 1,
    }


def test_build_job_config_csv_partition(tmp_path):
    folder = tmp_path / "events"
    partition = folder / "2026-08-04"
    partition.mkdir(parents=True)
    (partition / "schema.json").write_text(json.dumps(_schema()), encoding="utf-8")
    config = MagicMock()

    with patch.object(
        bqmod.bigquery, "SchemaField", side_effect=lambda **kwargs: kwargs
    ) as schema_field, patch.object(
        bqmod.bigquery, "LoadJobConfig", return_value=config
    ):
        result, uri = bqmod.BigQuery.build_job_config(
            table_name="analytics.events",
            bucket_name="bucket",
            data_path=f"{folder}/",
            partition_date=datetime.date(2026, 8, 4),
        )

    assert result is config
    assert uri == "gs://bucket/events/2026-08-04/*.csv.gz"
    assert config.write_disposition == bqmod.bigquery.WriteDisposition.WRITE_APPEND
    assert config.field_delimiter == ","
    assert config.skip_leading_rows == 1
    assert config.source_format == bqmod.bigquery.SourceFormat.CSV
    assert schema_field.call_count == 2


def test_build_job_config_json_without_partition(tmp_path):
    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "schema.json").write_text(
        json.dumps(_schema("NEWLINE_DELIMITED_JSON")), encoding="utf-8"
    )
    config = MagicMock()

    with patch.object(
        bqmod.bigquery, "SchemaField", side_effect=lambda **kwargs: kwargs
    ), patch.object(bqmod.bigquery, "LoadJobConfig", return_value=config):
        result, uri = bqmod.BigQuery.build_job_config(
            table_name="analytics.events",
            bucket_name="bucket",
            data_path=f"{folder}/",
            partition_date=None,
        )

    assert result is config
    assert uri == f"gs://bucket/{folder}/*.json.gz"
    assert config.write_disposition == bqmod.bigquery.WriteDisposition.WRITE_TRUNCATE
    assert config.source_format == bqmod.bigquery.SourceFormat.NEWLINE_DELIMITED_JSON


def test_build_job_config_downloads_missing_schema(tmp_path):
    folder = tmp_path / "events"
    folder.mkdir()
    partition = folder / "2026-08-04"
    partition.mkdir()
    schema_path = folder / "schema.json"

    storage = MagicMock()

    def download_schema(**kwargs):
        schema_path.write_text(json.dumps(_schema()), encoding="utf-8")

    storage.download_as_string.side_effect = download_schema
    config = MagicMock()

    with patch.object(bqmod, "CloudStorage", return_value=storage), patch.object(
        bqmod.bigquery, "SchemaField", side_effect=lambda **kwargs: kwargs
    ), patch.object(bqmod.bigquery, "LoadJobConfig", return_value=config):
        _, uri = bqmod.BigQuery.build_job_config(
            table_name="analytics.events",
            bucket_name="bucket",
            data_path=f"{folder}/",
            partition_date=datetime.date(2026, 8, 4),
        )

    storage.download_as_string.assert_called_once()
    assert (partition / "schema.json").exists()
    assert uri.endswith("/events/2026-08-04/*.csv.gz")


def test_load_from_cloud_deletes_partition_and_waits_for_job():
    bigquery = _bigquery_with_client()
    bigquery.delete_partition = MagicMock()
    job = MagicMock()
    bigquery._client.load_table_from_uri.return_value = job
    config = MagicMock()
    partition_date = datetime.date(2026, 8, 4)

    with patch.object(
        bqmod.BigQuery,
        "build_job_config",
        return_value=(config, "gs://bucket/events/*.csv.gz"),
    ) as build_config:
        result = bigquery.load_from_cloud(
            bucket_name="bucket",
            data_set="analytics",
            table="events",
            local_folder="unused/",
            remote_folder="events/",
            partition_date=partition_date,
            override=True,
        )

    assert result is True
    bigquery.delete_partition.assert_called_once_with(
        "analytics.events", partition_date, "date"
    )
    build_config.assert_called_once()
    bigquery._client.load_table_from_uri.assert_called_once_with(
        "gs://bucket/events/*.csv.gz",
        "analytics.events",
        job_config=config,
    )
    job.result.assert_called_once_with()


def test_load_from_uri_builds_config_and_waits_for_job():
    bigquery = _bigquery_with_client()
    job = MagicMock()
    bigquery._client.load_table_from_uri.return_value = job
    config = MagicMock()
    partition_date = datetime.date(2026, 8, 4)

    with patch.object(
        bqmod.BigQuery,
        "build_job_config",
        return_value=(config, "gs://bucket/events/*.json.gz"),
    ):
        result = bigquery.load_from_uri(
            table_id="analytics.events",
            bucket_name="bucket",
            data_path="events/",
            partition_date=partition_date,
        )

    assert result is True
    bigquery._client.load_table_from_uri.assert_called_once_with(
        source_uris="gs://bucket/events/*.json.gz",
        destination="analytics.events",
        job_config=config,
    )
    job.result.assert_called_once_with()


def test_bigquery_to_dataframe_returns_query_dataframe():
    bigquery = _bigquery_with_client()
    dataframe = MagicMock()
    query_result = bigquery._client.query.return_value.result.return_value
    query_result.to_dataframe.return_value = dataframe

    result = bigquery.bigquery_to_dataframe("SELECT 1")

    assert result is dataframe
    bigquery._client.query.assert_called_once_with("SELECT 1")
    query_result.to_dataframe.assert_called_once_with(create_bqstorage_client=True)
