import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
import os
import pandas as pd

from google.cloud.exceptions import NotFound

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.BigQuery import BigQuery


from google.auth.credentials import Credentials


class TestBigQuery(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.BigQuery.ServiceAccount", autospec=True)
    @patch("google.cloud.bigquery.Client")
    def test_init_with_credentials(self, mock_bigquery_client, mock_service_account):
        """Test that the BigQuery client is initialised with credentials."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_creds.project_id = "test-project"
        mock_service_account.from_service_account_file.return_value = mock_creds
        bq = BigQuery(credentials="path/to/creds.json")
        mock_bigquery_client.assert_called_once_with(
            credentials=mock_creds, project="test-project"
        )

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_init_without_credentials(self, mock_bigquery_client, mock_auth_default):
        """Test that the BigQuery client is initialised without credentials."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        bq = BigQuery()
        mock_bigquery_client.assert_called_once_with(
            credentials=mock_creds, project="test-project"
        )

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_table_exists_true(self, mock_bigquery_client, mock_auth_default):
        """Test the table_exists method when the table exists."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        self.assertTrue(bq.table_exists("my-project.my_dataset.my_table"))
        mock_client.get_table.assert_called_once_with("my-project.my_dataset.my_table")

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_table_exists_false(self, mock_bigquery_client, mock_auth_default):
        """Test the table_exists method when the table does not exist."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("Table not found")
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        self.assertFalse(bq.table_exists("my-project.my_dataset.my_table"))
        mock_client.get_table.assert_called_once_with("my-project.my_dataset.my_table")

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_execute_query(self, mock_bigquery_client, mock_auth_default):
        """Test the execute_query method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [("a", 1), ("b", 2)]
        mock_client.query.return_value = mock_query_job
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        results = bq.execute_query("SELECT * FROM my_table")
        self.assertEqual(len(results), 2)
        mock_client.query.assert_called_once_with("SELECT * FROM my_table")

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_delete_partition(self, mock_bigquery_client, mock_auth_default):
        """Test the delete_partition method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client

        # Mock table_exists to return True
        with patch.object(bq, "table_exists", return_value=True):
            result = bq.delete_partition("my.table", date(2023, 1, 1))
            self.assertTrue(result)
            expected_query = "DELETE FROM my.table WHERE date = '2023-01-01'"
            mock_client.query.assert_called_once_with(expected_query)
            mock_query_job.result.assert_called_once()

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_delete_partition_table_not_exists(
        self, mock_bigquery_client, mock_auth_default
    ):
        """Test delete_partition when the table does not exist."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client

        # Mock table_exists to return False
        with patch.object(bq, "table_exists", return_value=False):
            result = bq.delete_partition("my.table", date(2023, 1, 1))
            self.assertFalse(result)
            mock_client.query.assert_not_called()

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_execute_stored_procedure(self, mock_bigquery_client, mock_auth_default):
        """Test the execute_stored_procedure method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [{"a": 1, "b": 2}]
        mock_client.query.return_value = mock_query_job
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        sp_params = [
            BigQuery.oSpParam(name="param1", value="value1", type="STRING"),
            BigQuery.oSpParam(name="param2", value=123, type="INT64"),
        ]
        results = bq.execute_stored_procedure("my_sp", sp_params)
        self.assertEqual(results.to_dict(), {"a": {0: 1}, "b": {0: 2}})
        mock_client.query.assert_called_once()

    @patch("GoogleCloudPlatformAPI.BigQuery.CloudStorage")
    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    @patch.dict(os.environ, {"DEFAULT_BQ_DATASET": "my_dataset"})
    def test_create_schema_from_table(
        self, mock_bigquery_client, mock_auth_default, mock_cloud_storage
    ):
        """Test the create_schema_from_table method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_table = MagicMock()
        schema_field1 = MagicMock()
        schema_field1.name = "col1"
        schema_field1.field_type = "STRING"
        schema_field1.mode = "NULLABLE"
        schema_field2 = MagicMock()
        schema_field2.name = "col2"
        schema_field2.field_type = "INTEGER"
        schema_field2.mode = "REQUIRED"
        mock_table.schema = [
            schema_field1,
            schema_field2,
        ]
        mock_client.get_table.return_value = mock_table
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client

        with patch.object(bq, "table_exists", return_value=True):
            schema = bq.create_schema_from_table("my_table")
            self.assertIsNotNone(schema)
            mock_cloud_storage.return_value.upload_from_string.assert_called_once()

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_load_from_query(self, mock_bigquery_client, mock_auth_default):
        """Test the load_from_query method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_client.project = "test-project"
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        table_id = f"{mock_client.project}.my_dataset.my_table"
        bq.load_from_query("SELECT * FROM my_table", table_id)
        mock_client.query.assert_called_once()

    @patch("google.auth.default")
    @patch("google.cloud.bigquery.Client")
    def test_dataframe_to_bigquery(self, mock_bigquery_client, mock_auth_default):
        """Test the dataframe_to_bigquery method."""
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.universe_domain = "googleapis.com"
        mock_auth_default.return_value = (mock_creds, "test-project")
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.num_rows = 1
        mock_table.schema = []
        mock_client.get_table.return_value = mock_table
        mock_bigquery_client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        df = pd.DataFrame({"a": [1], "b": ["2"]})
        bq.dataframe_to_bigquery(df, "my_dataset.my_table")
        mock_client.load_table_from_dataframe.assert_called_once()


if __name__ == "__main__":
    unittest.main()
