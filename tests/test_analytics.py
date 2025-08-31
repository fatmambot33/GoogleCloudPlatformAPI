import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importlib
analytics_mod = importlib.import_module("GoogleCloudPlatformAPI.Analytics")


def test_list_views():
    with patch.object(analytics_mod.Analytics, "ServiceAccount") as mock_service_account, \
        patch.object(analytics_mod, "build") as mock_build:
        # Configure build() to return a fake management client whose call chain returns items
        fake_mgmt = MagicMock()
        mock_build.return_value = fake_mgmt
        fake_mgmt.management.return_value.profiles.return_value.list.return_value.execute.return_value = {
            "items": ["view1", "view2"]
        }

        analytics = analytics_mod.Analytics(credentials="dummy_creds.json")
        views = analytics.list_views()

        assert len(views) == 2
        assert views == ["view1", "view2"]
        fake_mgmt.management.return_value.profiles.return_value.list.assert_called_once_with(
            accountId="~all", webPropertyId="~all"
        )


def test_get_report():
    with patch.object(analytics_mod.Analytics, "ServiceAccount") as mock_service_account, \
        patch.object(analytics_mod, "build") as mock_build:
        # Configure build() to return a fake reporting client
        fake_reporting = MagicMock()
        mock_build.return_value = fake_reporting
        mock_report = {
            "reports": [
                {
                    "columnHeader": {
                        "dimensions": ["ga:date"],
                        "metricHeader": {"metricHeaderEntries": [{"name": "ga:sessions"}]},
                    },
                    "data": {
                        "rows": [
                            {"dimensions": ["20230101"], "metrics": [{"values": ["100"]}]}
                        ]
                    },
                }
            ]
        }
        fake_reporting.reports.return_value.batchGet.return_value.execute.return_value = mock_report

        analytics = analytics_mod.Analytics(credentials="dummy_creds.json")
        df = analytics.get_report(12345)
        assert df.shape == (1, 2)
        assert df.iloc[0]["sessions"] == 100
