import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.Analytics import Analytics


class TestAnalytics(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.Analytics.ServiceAccount")
    @patch("googleapiclient.discovery.build")
    def test_list_views(self, mock_build, mock_service_account):
        """Test the list_views method."""
        analytics = Analytics(credentials="dummy_creds.json")
        mock_management_service = MagicMock()
        analytics._Analytics__management = mock_management_service

        mock_profiles_list = MagicMock()
        mock_profiles_list.execute.return_value = {"items": ["view1", "view2"]}
        mock_management_service.management.return_value.profiles.return_value.list.return_value = (
            mock_profiles_list
        )

        views = analytics.list_views()

        self.assertEqual(len(views), 2)
        self.assertEqual(views, ["view1", "view2"])
        mock_management_service.management.return_value.profiles.return_value.list.assert_called_once_with(
            accountId="~all", webPropertyId="~all"
        )


    @patch("GoogleCloudPlatformAPI.Analytics.ServiceAccount")
    @patch("googleapiclient.discovery.build")
    def test_get_report(self, mock_build, mock_service_account):
        """Test the get_report method."""
        analytics = Analytics(credentials="dummy_creds.json")
        mock_reporting_service = MagicMock()
        analytics._Analytics__reporting = mock_reporting_service

        mock_report = {
            "reports": [
                {
                    "columnHeader": {
                        "dimensions": ["ga:date"],
                        "metricHeader": {
                            "metricHeaderEntries": [{"name": "ga:sessions"}]
                        },
                    },
                    "data": {
                        "rows": [
                            {
                                "dimensions": ["20230101"],
                                "metrics": [{"values": ["100"]}],
                            }
                        ]
                    },
                }
            ]
        }
        mock_reporting_service.reports.return_value.batchGet.return_value.execute.return_value = (
            mock_report
        )

        df = analytics.get_report(12345)
        self.assertEqual(df.shape, (1, 2))
        self.assertEqual(df.iloc[0]["sessions"], 100)


if __name__ == "__main__":
    unittest.main()
