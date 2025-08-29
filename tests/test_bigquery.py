import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import NotFound

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.BigQuery import BigQuery


class TestBigQuery(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.BigQuery.ServiceAccount")
    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_init_with_credentials(self, mock_bigquery, mock_service_account):
        """Test that the BigQuery client is initialised with credentials."""
        mock_creds = MagicMock()
        mock_service_account.from_service_account_file.return_value = mock_creds
        bq = BigQuery(credentials="path/to/creds.json", project_id="test-project")
        mock_bigquery.Client.assert_called_once_with(
            credentials=mock_creds, project="test-project"
        )

    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_init_without_credentials(self, mock_bigquery):
        """Test that the BigQuery client is initialised without credentials."""
        bq = BigQuery(project_id="test-project")
        mock_bigquery.Client.assert_called_once_with(project="test-project")

    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_table_exists_true(self, mock_bigquery):
        """Test the table_exists method when the table exists."""
        mock_client = MagicMock()
        mock_bigquery.Client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        self.assertTrue(bq.table_exists("my-project.my_dataset.my_table"))
        mock_client.get_table.assert_called_once_with("my-project.my_dataset.my_table")

    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_table_exists_false(self, mock_bigquery):
        """Test the table_exists method when the table does not exist."""
        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("Table not found")
        mock_bigquery.Client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        self.assertFalse(bq.table_exists("my-project.my_dataset.my_table"))
        mock_client.get_table.assert_called_once_with("my-project.my_dataset.my_table")

    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_execute_query(self, mock_bigquery):
        """Test the execute_query method."""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [("a", 1), ("b", 2)]
        mock_client.query.return_value = mock_query_job
        mock_bigquery.Client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client
        results = bq.execute_query("SELECT * FROM my_table")
        self.assertEqual(len(results), 2)
        mock_client.query.assert_called_once_with("SELECT * FROM my_table")

    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_delete_partition(self, mock_bigquery):
        """Test the delete_partition method."""
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_bigquery.Client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client

        # Mock table_exists to return True
        with patch.object(bq, "table_exists", return_value=True):
            result = bq.delete_partition("my.table", date(2023, 1, 1))
            self.assertTrue(result)
            expected_query = "DELETE FROM my.table WHERE date = '2023-01-01'"
            mock_client.query.assert_called_once_with(expected_query)
            mock_query_job.result.assert_called_once()

    @patch("GoogleCloudPlatformAPI.BigQuery.bigquery")
    def test_delete_partition_table_not_exists(self, mock_bigquery):
        """Test delete_partition when the table does not exist."""
        mock_client = MagicMock()
        mock_bigquery.Client.return_value = mock_client
        bq = BigQuery()
        bq._BigQuery__client = mock_client

        # Mock table_exists to return False
        with patch.object(bq, "table_exists", return_value=False):
            result = bq.delete_partition("my.table", date(2023, 1, 1))
            self.assertFalse(result)
            mock_client.query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
