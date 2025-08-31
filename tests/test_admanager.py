import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.AdManager import (
    AudienceService,
    CustomTargetingService,
    ForecastService,
    ReportService,
)


class TestAdManager(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_audience_service_list(self, mock_gam_client):
        """Test the list method of the AudienceService."""
        # Arrange
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        # Simulate two pages of results
        mock_service.getAudienceSegmentsByStatement.side_effect = [
            {
                "results": [
                    {"id": 1, "name": "Segment 1"},
                    {"id": 2, "name": "Segment 2"},
                ]
            },
            {"results": [{"id": 3, "name": "Segment 3"}]},
            {"results": []},  # Empty page to terminate the loop
        ]

        # Act
        audience_service = AudienceService()
        results = audience_service.list()

        # Assert
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["name"], "Segment 1")
        self.assertEqual(results[2]["name"], "Segment 3")
        self.assertEqual(mock_service.getAudienceSegmentsByStatement.call_count, 3)


    @patch("GoogleCloudPlatformAPI.AdManager.NetworkService")
    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_audience_service_create(self, mock_gam_client, mock_network_service):
        """Test the create method of the AudienceService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance
        mock_network_service.return_value.effectiveRootAdUnitId.return_value = "root"

        audience_service = AudienceService()
        audience_service.create("name", "description", {})
        mock_service.createAudienceSegments.assert_called_once()


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_audience_service_update(self, mock_gam_client):
        """Test the update method of the AudienceService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        mock_service.getAudienceSegmentsByStatement.return_value = {
            "results": [{"id": 1, "name": "Segment 1"}]
        }

        audience_service = AudienceService()
        audience_service.update(1, "new name", "new description", 1, 1)
        mock_service.updateAudienceSegments.assert_called_once()


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_audience_service_list_all(self, mock_gam_client):
        """Test the list_all method of the AudienceService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        mock_service.getAudienceSegmentsByStatement.side_effect = [
            {
                "results": [
                    {"id": 1, "name": "Segment 1", "size": 100},
                    {"id": 2, "name": "Segment 2", "size": 200},
                ]
            },
            {"results": [{"id": 3, "name": "Segment 3", "size": 300}]},
            {"results": []},
        ]

        audience_service = AudienceService()
        results = audience_service.list_all()

        self.assertEqual(len(results), 3)
        self.assertEqual(mock_service.getAudienceSegmentsByStatement.call_count, 3)


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_custom_targeting_service_list(self, mock_gam_client):
        """Test the list method of the CustomTargetingService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        mock_service.getCustomTargetingValuesByStatement.side_effect = [
            {"results": [{"id": 1, "name": "Value 1"}]},
            {"results": []},
        ]

        custom_targeting_service = CustomTargetingService()
        results = custom_targeting_service.list(1)

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_service.getCustomTargetingValuesByStatement.call_count, 2)


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_custom_targeting_service_delete(self, mock_gam_client):
        """Test the delete method of the CustomTargetingService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        custom_targeting_service = CustomTargetingService()
        custom_targeting_service.delete(1, [{"id": 1, "name": "test"}])
        mock_service.performCustomTargetingValueAction.assert_called_once()


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_report_service_get_report_dataframe(self, mock_gam_client):
        """Test the get_report_dataframe method of the ReportService."""
        mock_data_downloader = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_data_downloader.return_value = mock_data_downloader
        mock_gam_client.return_value = mock_gam_instance

        report_service = ReportService()
        with patch.object(
            report_service, "_ReportService__get_report_by_report_job"
        ) as mock_get_report:
            mock_get_report.return_value = [
                {"Dimension.DATE": "2023-01-01", "Column.IMPRESSIONS": "100"}
            ]
            df = report_service.get_report_dataframe()
            self.assertEqual(df.shape, (1, 2))


    @patch("tempfile.NamedTemporaryFile")
    @patch("gzip.open")
    @patch("builtins.open")
    @patch("os.remove")
    @patch("shutil.copyfileobj")
    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_get_report_by_report_job(
        self,
        mock_gam_client,
        mock_copyfileobj,
        mock_remove,
        mock_open,
        mock_gzip_open,
        mock_tempfile,
    ):
        """Test the __get_report_by_report_job method."""
        mock_data_downloader = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_data_downloader.return_value = mock_data_downloader
        mock_gam_client.return_value = mock_gam_instance

        report_service = ReportService()
        report_service._ReportService__get_report_by_report_job({})
        mock_data_downloader.WaitForReport.assert_called_once()
        mock_data_downloader.DownloadReportToFile.assert_called_once()


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_forecast_service_get_forecast(self, mock_gam_client):
        """Test the get_forecast method of the ForecastService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        mock_service.getAvailabilityForecast.return_value = {
            "breakdowns": [
                {
                    "startTime": {"date": {"year": 2023, "month": 1, "day": 1}},
                    "breakdownEntries": [
                        {
                            "forecast": {"matched": 100, "available": 200},
                            "name": "target1",
                        }
                    ],
                }
            ]
        }

        forecast_service = ForecastService()
        results = forecast_service.get_forecast([], [], [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["matched"], 100)


    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_forecast_service_get_forecast_by_targeting_preset(self, mock_gam_client):
        """Test the get_forecast_by_targeting_preset method of the ForecastService."""
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        mock_service.getAvailabilityForecast.return_value = {
            "breakdowns": [
                {
                    "startTime": {"date": {"year": 2023, "month": 1, "day": 1}},
                    "breakdownEntries": [
                        {
                            "forecast": {"matched": 100, "available": 200},
                            "name": "target1",
                        }
                    ],
                }
            ]
        }

        forecast_service = ForecastService()
        results = forecast_service.get_forecast_by_targeting_preset([], [], [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["matched"], 100)


if __name__ == "__main__":
    unittest.main()
