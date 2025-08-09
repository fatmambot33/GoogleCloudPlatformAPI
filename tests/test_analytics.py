import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.Analytics import Analytics


class TestAnalytics(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.Analytics.ServiceAccount")
    @patch("GoogleCloudPlatformAPI.Analytics.build")
    def test_list_views(self, mock_build, mock_service_account):
        """Test the list_views method."""
        mock_reporting_service = MagicMock()
        mock_management_service = MagicMock()

        def build_side_effect(serviceName, version, credentials):
            if serviceName == "analyticsreporting":
                return mock_reporting_service
            elif serviceName == "analytics":
                return mock_management_service
            return MagicMock()

        mock_build.side_effect = build_side_effect

        mock_profiles = MagicMock()
        mock_management_service.management.return_value.profiles.return_value = mock_profiles
        mock_profiles.list.return_value.execute.return_value = {"items": ["view1", "view2"]}

        analytics = Analytics(credentials="dummy_creds.json")
        views = analytics.list_views()

        self.assertEqual(len(views), 2)
        self.assertEqual(views, ["view1", "view2"])
        mock_service_account.from_service_account_file.assert_called_once()
        mock_profiles.list.assert_called_once_with(accountId="~all", webPropertyId="~all")


if __name__ == "__main__":
    unittest.main()
