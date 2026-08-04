"""Deterministic tests for Analytics service boundaries."""

import datetime
import importlib
from unittest.mock import MagicMock, call, patch

import pandas as pd

analyticsmod = importlib.import_module("GoogleCloudPlatformAPI.Analytics")


def _analytics_without_init():
    analytics = analyticsmod.Analytics.__new__(analyticsmod.Analytics)
    analytics._reporting = MagicMock()
    analytics._management = MagicMock()
    return analytics


def test_init_builds_reporting_and_management_clients(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "analytics.json")
    credential = MagicMock()
    reporting = MagicMock()
    management = MagicMock()

    with (
        patch.object(
            analyticsmod.Analytics.ServiceAccount,
            "from_service_account_file",
            return_value=credential,
        ) as from_file,
        patch.object(
            analyticsmod,
            "build",
            side_effect=[reporting, management],
        ) as build,
    ):
        analytics = analyticsmod.Analytics()

    assert analytics._reporting is reporting
    assert analytics._management is management
    from_file.assert_called_once_with(
        credentials="analytics.json", scopes=analyticsmod.Analytics.SCOPES
    )
    assert build.call_args_list == [
        call("analyticsreporting", "v4", credentials=credential),
        call("analytics", "v3", credentials=credential),
    ]


def test_list_views_returns_items_and_defaults_to_empty_list():
    analytics = _analytics_without_init()
    profiles = analytics._management.management.return_value.profiles.return_value
    list_views = profiles.list
    execute = list_views.return_value.execute
    execute.side_effect = [{"items": [{"id": "1"}]}, {}]

    assert analytics.list_views() == [{"id": "1"}]
    assert analytics.list_views() == []
    list_views.assert_called_with(accountId="~all", webPropertyId="~all")


def test_get_report_normalizes_dates_and_builds_request_body():
    analytics = _analytics_without_init()
    reports = analytics._reporting.reports.return_value
    execute = reports.batchGet.return_value.execute
    execute.return_value = {"reports": []}

    result = analytics._get_report(
        view_id=123,
        dimensions=["ga:date"],
        metrics=["ga:sessions"],
        start_date=datetime.date(2026, 1, 1),
        end_date=datetime.date(2026, 1, 31),
    )

    assert result == {"reports": []}
    reports.batchGet.assert_called_once_with(
        body={
            "reportRequests": [
                {
                    "viewId": "123",
                    "dateRanges": [
                        {"startDate": "2026-01-01", "endDate": "2026-01-31"}
                    ],
                    "metrics": [{"expression": "ga:sessions"}],
                    "dimensions": [{"name": "ga:date"}],
                    "pageSize": 100000,
                }
            ]
        }
    )


def test_get_report_converts_api_response_to_dataframe():
    analytics = _analytics_without_init()
    response = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:source"],
                    "metricHeader": {
                        "metricHeaderEntries": [{"name": "ga:sessions"}]
                    },
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["search"],
                            "metrics": [{"values": ["12"]}],
                        }
                    ]
                },
            }
        ]
    }
    analytics._get_report = MagicMock(return_value=response)

    result = analytics.get_report(view_id=42)

    assert result.to_dict("records") == [{"source": "search", "sessions": 12}]
    analytics._get_report.assert_called_once_with(
        view_id=42,
        dimensions=["ga:source", "ga:medium"],
        metrics=["ga:sessions"],
        start_date="30daysAgo",
        end_date="yesterday",
    )


def test_get_all_reports_adds_view_metadata_and_concatenates():
    analytics = _analytics_without_init()
    analytics.list_views = MagicMock(
        return_value=[
            {
                "id": "11",
                "name": "Primary",
                "accountId": "account",
                "webPropertyId": "property",
            }
        ]
    )
    analytics.get_report = MagicMock(
        return_value=pd.DataFrame([{"source": "direct", "sessions": 3}])
    )

    result = analytics.get_all_reports()

    assert result.to_dict("records") == [
        {
            "source": "direct",
            "sessions": 3,
            "view_id": "11",
            "view_name": "Primary",
            "view_accountId": "account",
            "view_webPropertyId": "property",
        }
    ]


def test_report_to_df_converts_dates_metrics_and_invalid_values():
    response = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:date", "ga:source"],
                    "metricHeader": {
                        "metricHeaderEntries": [
                            {"name": "ga:sessions"},
                            {"name": "ga:bounceRate"},
                        ]
                    },
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["20260804", "search"],
                            "metrics": [{"values": ["7", "invalid"]}],
                        }
                    ]
                },
            }
        ]
    }

    result = analyticsmod.Analytics.report_to_df(response)

    assert result.loc[0, "date"] == pd.Timestamp("2026-08-04")
    assert result.loc[0, "source"] == "search"
    assert result.loc[0, "sessions"] == 7
    assert pd.isna(result.loc[0, "bounceRate"])


def test_report_to_df_supports_empty_rows():
    response = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:source"],
                    "metricHeader": {
                        "metricHeaderEntries": [{"name": "ga:sessions"}]
                    },
                },
                "data": {},
            }
        ]
    }

    result = analyticsmod.Analytics.report_to_df(response)

    assert list(result.columns) == ["source", "sessions"]
    assert result.empty
