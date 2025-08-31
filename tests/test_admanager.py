import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importlib

ad_mod = importlib.import_module("GoogleCloudPlatformAPI.AdManager")


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_audience_service_list(mock_gam_client):
    # Arrange
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance

    # Simulate pages of results
    mock_service.getAudienceSegmentsByStatement.side_effect = [
        {"results": [{"id": 1, "name": "Segment 1"}, {"id": 2, "name": "Segment 2"}]},
        {"results": [{"id": 3, "name": "Segment 3"}]},
        {"results": []},
    ]

    audience_service = ad_mod.AudienceService()
    results = audience_service.list()

    assert len(results) == 3
    assert results[0]["name"] == "Segment 1"
    assert results[2]["name"] == "Segment 3"
    assert mock_service.getAudienceSegmentsByStatement.call_count == 3


@patch("GoogleCloudPlatformAPI.AdManager.NetworkService")
@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_audience_service_create(mock_gam_client, mock_network_service):
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance
    mock_network_service.return_value.effectiveRootAdUnitId.return_value = "root"

    audience_service = ad_mod.AudienceService()
    audience_service.create("name", "description", {})
    mock_service.createAudienceSegments.assert_called_once()


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_audience_service_update(mock_gam_client):
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance

    mock_service.getAudienceSegmentsByStatement.return_value = {
        "results": [{"id": 1, "name": "Segment 1"}]
    }

    audience_service = ad_mod.AudienceService()
    audience_service.update(1, "new name", "new description", 1, 1)
    mock_service.updateAudienceSegments.assert_called_once()


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_audience_service_list_all(mock_gam_client):
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

    audience_service = ad_mod.AudienceService()
    results = audience_service.list_all()

    assert len(results) == 3
    assert mock_service.getAudienceSegmentsByStatement.call_count == 3


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_custom_targeting_service_list(mock_gam_client):
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance

    mock_service.getCustomTargetingValuesByStatement.side_effect = [
        {"results": [{"id": 1, "name": "Value 1"}]},
        {"results": []},
    ]

    custom_targeting_service = ad_mod.CustomTargetingService()
    results = custom_targeting_service.list(1)

    assert len(results) == 1
    assert mock_service.getCustomTargetingValuesByStatement.call_count == 2


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_custom_targeting_service_delete(mock_gam_client):
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance

    custom_targeting_service = ad_mod.CustomTargetingService()
    custom_targeting_service.delete(1, [{"id": 1, "name": "test"}])
    assert mock_service.performCustomTargetingValueAction.called


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_report_service_get_report_dataframe(mock_gam_client):
    mock_data_downloader = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_data_downloader.return_value = mock_data_downloader
    mock_gam_client.return_value = mock_gam_instance

    report_service = ad_mod.ReportService()
    with patch.object(
        ad_mod.ReportService, "get_report_dataframe_by_statement"
    ) as mock_get_by_stmt:
        mock_get_by_stmt.return_value = pd.DataFrame(
            {"Dimension.DATE": ["2023-01-01"], "Column.IMPRESSIONS": ["100"]}
        )
        df = report_service.get_report_dataframe()
        assert df.shape == (1, 2)


@patch("tempfile.NamedTemporaryFile")
@patch("gzip.open")
@patch("builtins.open")
@patch("os.remove")
@patch("shutil.copyfileobj")
@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_get_report_via_public_method(
    mock_gam_client,
    mock_copyfileobj,
    mock_remove,
    mock_open,
    mock_gzip_open,
    mock_tempfile,
):
    mock_data_downloader = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_data_downloader.return_value = mock_data_downloader
    mock_gam_client.return_value = mock_gam_instance

    # Prepare gzip/open mocks so file operations do not fail
    mock_gzip_open.return_value.__enter__.return_value = MagicMock()
    mock_open.return_value.__enter__.return_value = MagicMock()

    report_service = ad_mod.ReportService()
    _ = report_service.get_report_dataframe_by_statement(
        statement=ad_mod.ad_manager.StatementBuilder(version=ad_mod.GAM_VERSION),
        report_date=datetime.date.today(),
        days=1,
    )
    assert mock_data_downloader.WaitForReport.called
    assert mock_data_downloader.DownloadReportToFile.called


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_forecast_service_get_forecast(mock_gam_client):
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance

    mock_service.getAvailabilityForecast.return_value = {
        "breakdowns": [
            {
                "startTime": {"date": {"year": 2023, "month": 1, "day": 1}},
                "breakdownEntries": [
                    {"forecast": {"matched": 100, "available": 200}, "name": "target1"}
                ],
            }
        ]
    }

    forecast_service = ad_mod.ForecastService()
    results = forecast_service.get_forecast([], [], [])
    assert len(results) == 1
    assert results[0]["matched"] == 100


@patch("GoogleCloudPlatformAPI.AdManager.GamClient")
def test_forecast_service_get_forecast_by_targeting_preset(mock_gam_client):
    mock_service = MagicMock()
    mock_gam_instance = MagicMock()
    mock_gam_instance.get_service.return_value = mock_service
    mock_gam_client.return_value = mock_gam_instance

    mock_service.getAvailabilityForecast.return_value = {
        "breakdowns": [
            {
                "startTime": {"date": {"year": 2023, "month": 1, "day": 1}},
                "breakdownEntries": [
                    {"forecast": {"matched": 100, "available": 200}, "name": "target1"}
                ],
            }
        ]
    }

    forecast_service = ad_mod.ForecastService()
    results = forecast_service.get_forecast_by_targeting_preset([], [], [])
    assert len(results) == 1
    assert results[0]["matched"] == 100
