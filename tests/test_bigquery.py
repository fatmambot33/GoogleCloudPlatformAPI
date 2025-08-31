import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from google.auth.credentials import Credentials
from google.cloud.exceptions import NotFound

sys.path.append(str(Path(__file__).resolve().parents[1]))
import importlib

bqmod = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")


@pytest.fixture
def mock_client():
    client = MagicMock()
    return client


def test_init_with_credentials(mock_client):
    with patch.object(bqmod, "ServiceAccount", autospec=True) as mock_sa, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ) as mock_bq_client:
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_creds.project_id = "test-project"
        mock_sa.from_service_account_file.return_value = mock_creds

        bqmod.BigQuery(credentials="path/to/creds.json")

        mock_bq_client.assert_called_once_with(
            credentials=mock_creds, project="test-project"
        )


def test_init_without_credentials(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ) as mock_bq_client:
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")

        bqmod.BigQuery()

        mock_bq_client.assert_called_once_with(
            credentials=mock_creds, project="test-project"
        )


def test_table_exists_true(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")

        bq = bqmod.BigQuery()
        assert bq.table_exists("my-project.my_dataset.my_table") is True
        mock_client.get_table.assert_called_once_with("my-project.my_dataset.my_table")


def test_table_exists_false(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client.get_table.side_effect = NotFound("Table not found")

        bq = bqmod.BigQuery()
        assert bq.table_exists("my-project.my_dataset.my_table") is False
        mock_client.get_table.assert_called_once_with("my-project.my_dataset.my_table")


def test_execute_query(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [("a", 1), ("b", 2)]
        mock_client.query.return_value = mock_query_job

        bq = bqmod.BigQuery()
        results = bq.execute_query("SELECT * FROM my_table")
        assert len(results) == 2
        mock_client.query.assert_called_once_with("SELECT * FROM my_table")


def test_delete_partition(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job

        bq = bqmod.BigQuery()
        with patch.object(bq, "table_exists", return_value=True):
            result = bq.delete_partition("my.table", date(2023, 1, 1))
            assert result is True
            expected_query = "DELETE FROM my.table WHERE date = '2023-01-01'"
            mock_client.query.assert_called_once_with(expected_query)
            mock_query_job.result.assert_called_once()


def test_delete_partition_table_not_exists(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")

        bq = bqmod.BigQuery()
        with patch.object(bq, "table_exists", return_value=False):
            result = bq.delete_partition("my.table", date(2023, 1, 1))
            assert result is False
            mock_client.query.assert_not_called()


def test_execute_stored_procedure(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [{"a": 1, "b": 2}]
        mock_client.query.return_value = mock_query_job

        bq = bqmod.BigQuery()
        sp_params = [
            bqmod.BigQuery.oSpParam(name="param1", value="value1", type="STRING"),
            bqmod.BigQuery.oSpParam(name="param2", value=123, type="INT64"),
        ]
        results = bq.execute_stored_procedure("my_sp", sp_params)
        assert results.to_dict() == {"a": {0: 1}, "b": {0: 2}}
        mock_client.query.assert_called()


def test_create_schema_from_table(mock_client, monkeypatch, tmp_path):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ), patch.dict(
        os.environ,
        {"DEFAULT_BQ_DATASET": "my_dataset", "DEFAULT_GCS_BUCKET": "bucket"},
        clear=False,
    ), patch.object(
        bqmod, "CloudStorage"
    ) as mock_cs:
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")

        mock_table = MagicMock()
        schema_field1 = MagicMock()
        schema_field1.name = "col1"
        schema_field1.field_type = "STRING"
        schema_field1.mode = "NULLABLE"
        schema_field2 = MagicMock()
        schema_field2.name = "col2"
        schema_field2.field_type = "INTEGER"
        schema_field2.mode = "REQUIRED"
        mock_table.schema = [schema_field1, schema_field2]
        mock_client.get_table.return_value = mock_table

        bq = bqmod.BigQuery()
        # Force table_exists True
        monkeypatch.setattr(bq, "table_exists", lambda _: True)
        schema = bq.create_schema_from_table("my_table")
        assert schema is not None
        mock_cs.return_value.upload_from_string.assert_called_once()


def test_load_from_query(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client.project = "test-project"

        bq = bqmod.BigQuery()
        table_id = f"{mock_client.project}.my_dataset.my_table"
        bq.load_from_query("SELECT * FROM my_table", table_id)
        mock_client.query.assert_called()


def test_dataframe_to_bigquery(mock_client):
    with patch("google.auth.default") as mock_auth_default, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_table = MagicMock()
        mock_table.num_rows = 1
        mock_table.schema = []
        mock_client.get_table.return_value = mock_table

        bq = bqmod.BigQuery()
        df = pd.DataFrame({"a": [1], "b": ["2"]})
        bq.dataframe_to_bigquery(df, "my_dataset.my_table")
        mock_client.load_table_from_dataframe.assert_called_once()
