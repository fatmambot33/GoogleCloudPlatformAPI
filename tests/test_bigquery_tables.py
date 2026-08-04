"""Deterministic tests for BigQuery table creation boundaries."""

import importlib
import json
from unittest.mock import MagicMock, patch

import pytest

bqmod = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")


def _bigquery_with_client():
    instance = bqmod.BigQuery.__new__(bqmod.BigQuery)
    instance._client = MagicMock()
    return instance


def _schema(source_format="CSV"):
    return {
        "source_format": source_format,
        "field_delimiter": ",",
        "skip_leading_rows": 1,
        "allow_jagged_rows": True,
        "allow_quoted_newlines": True,
        "table_schema": [
            {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "value", "type": "STRING", "mode": "NULLABLE"},
        ],
    }


def test_create_external_csv_table_configures_schema_and_partitioning():
    bigquery = _bigquery_with_client()
    dataset = MagicMock()
    table_reference = MagicMock()
    dataset.table.return_value = table_reference
    bigquery._client.dataset.return_value = dataset

    external_config = MagicMock()
    csv_options = MagicMock()
    table = MagicMock()

    with (
        patch.object(
            bqmod.bigquery, "SchemaField", side_effect=lambda **value: value
        ),
        patch.object(
            bqmod.bigquery, "ExternalConfig", return_value=external_config
        ),
        patch.object(bqmod.bigquery, "CSVOptions", return_value=csv_options),
        patch.object(bqmod.bigquery, "Table", return_value=table),
        patch.object(
            bqmod.bigquery, "TimePartitioning", return_value="partitioning"
        ),
    ):
        result = bigquery.create_external_table(
            "analytics", "events", _schema(), ["gs://bucket/events/*.csv"]
        )

    assert result is True
    assert external_config.source_uris == ["gs://bucket/events/*.csv"]
    assert csv_options.field_delimiter == ","
    assert csv_options.skip_leading_rows == 1
    assert csv_options.allow_jagged_rows is True
    assert csv_options.allow_quoted_newlines is True
    assert external_config.csv_options is csv_options
    assert table.time_partitioning == "partitioning"
    assert table.external_data_configuration is external_config
    bigquery._client.create_table.assert_called_once_with(table)


def test_create_external_json_table_returns_false_without_creating_table():
    bigquery = _bigquery_with_client()

    with (
        patch.object(
            bqmod.bigquery, "SchemaField", side_effect=lambda **value: value
        ),
        patch.object(bqmod.bigquery, "ExternalConfig", return_value=MagicMock()),
    ):
        result = bigquery.create_external_table(
            "analytics",
            "events",
            _schema("NEWLINE_DELIMITED_JSON"),
            ["gs://bucket/events/*.json"],
        )

    assert result is False
    bigquery._client.create_table.assert_not_called()


def test_create_table_from_schema_uses_report_date_partition(tmp_path):
    bigquery = _bigquery_with_client()
    bigquery.table_exists = MagicMock(return_value=False)
    dataset = MagicMock()
    dataset.table.return_value = "analytics.events"
    bigquery._client.dataset.return_value = dataset

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "schema.json").write_text(json.dumps(_schema()), encoding="utf-8")

    table = MagicMock()
    with (
        patch.object(
            bqmod.bigquery, "SchemaField", side_effect=lambda **value: value
        ),
        patch.object(bqmod.bigquery, "Table", return_value=table),
        patch.object(
            bqmod.bigquery,
            "TimePartitioning",
            return_value="report-date-partition",
        ),
    ):
        result = bigquery.create_table_from_schema(
            "events", dataset="analytics", data_path=f"{tmp_path}/"
        )

    assert result is True
    assert table.time_partitioning == "report-date-partition"
    bigquery._client.dataset.assert_called_once_with("analytics")
    bigquery._client.create_table.assert_called_once_with(table)


def test_create_table_from_schema_uses_environment_defaults(tmp_path, monkeypatch):
    bigquery = _bigquery_with_client()
    bigquery.table_exists = MagicMock(return_value=False)
    dataset = MagicMock()
    dataset.table.return_value = "analytics.events"
    bigquery._client.dataset.return_value = dataset

    folder = tmp_path / "events"
    folder.mkdir()
    schema = _schema()
    schema["table_schema"][0]["name"] = "date"
    (folder / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
    monkeypatch.setenv("DEFAULT_BQ_DATASET", "analytics")
    monkeypatch.setenv("DATA_PATH", f"{tmp_path}/")

    table = MagicMock()
    with (
        patch.object(
            bqmod.bigquery, "SchemaField", side_effect=lambda **value: value
        ),
        patch.object(bqmod.bigquery, "Table", return_value=table),
        patch.object(
            bqmod.bigquery, "TimePartitioning", return_value="date-partition"
        ),
    ):
        result = bigquery.create_table_from_schema("events")

    assert result is True
    assert table.time_partitioning == "date-partition"
    bigquery.table_exists.assert_called_once_with("analytics.events")


def test_create_table_from_schema_skips_existing_table():
    bigquery = _bigquery_with_client()
    bigquery.table_exists = MagicMock(return_value=True)

    result = bigquery.create_table_from_schema(
        "events", dataset="analytics", data_path="unused/"
    )

    assert result is False
    bigquery._client.create_table.assert_not_called()


def test_create_table_from_schema_propagates_missing_schema_file(tmp_path):
    bigquery = _bigquery_with_client()
    bigquery.table_exists = MagicMock(return_value=False)

    with pytest.raises(FileNotFoundError):
        bigquery.create_table_from_schema(
            "missing", dataset="analytics", data_path=f"{tmp_path}/"
        )
