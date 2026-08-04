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


def test_init_uses_environment_credentials_and_builds_clients(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
    credential = MagicMock()
    reporting = MagicMock()
    management = MagicMock()

    with patch.object(
        analyticsmod.Analytics.ServiceAccount,
        "from_service_account_file",
        return_value=credential,
    ) as from_file, patch.object(
        analyticsmod, "build", side_effect=[reporting, management]
    ) as build:
        analytics = analyticsmod.Analytics()

    assert analytics._reporting is reporting
    assert analytics._management is management
    from_file.assert_called_once_with(
        credentials="service-account.json", scopes=analyticsmod.Analytics.SCOPES
    )
    assert build.call_args_list == [
        call("analyticsreporting", "v4", credentials=credential),
        call("analytics", "v3", credentials=credential),
    ]


def test_list_views_returns_items_and_defaults_to_empty_list():
    analytics = _analytics_without_init()
    request = analytics._management.management.return_value.profiles.return_value.list
    request.return_value.execute.side_effect = [{"items": [{"id": "1"}]}, {}]

    assert analytics.list_views() == [{"id": "1"}]
    assert analytics.list_views() == []
    assert request.call_args_list == [
        call(accountId="~all", webPropertyId="~all"),
        call(accountId="~all", webPropertyId="~all"),
    ]


def test_get_report_formats_dates_and_builds_request_body():
    analytics = _analytics_without_init()
    request = analytics._reporting.reports.return_value.batchGet
    response = {"reports": []}
    request.return_value.execute.return_value = response

    result = analytics._get_report(
        view_id=123,
        dimensions=["ga:date", "ga:source"],
        metrics=["ga:sessions", "ga:users"],
        start_date=datetime.date(2026, 1, 2),
        end_date=datetime.date(2026, 1, 3),
    )

    assert result is response
    request.assert_called_once_with(
        body={
            "reportRequests": [
                {
                    "viewId": "123",
                    "dateRanges": [
                        {"startDate": "2026-01-02", "endDate": "2026-01-03"}
                    ],
                    "metrics": [
                        {"expression": "ga:sessions"},
                        {"expression": "ga:users"},
                    ],
                    "dimensions": [
                        {"name": "ga:date"},
                        {"name": "ga:source"},
                    ],
                    "pageSize": 100000,
                }
            ]
        }
    )


def test_get_report_converts_raw_response_to_dataframe():
    analytics = _analytics_without_init()
    raw = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:date", "ga:source"],
                    "metricHeader": {
                        "metricHeaderEntries": [{"name": "ga:sessions"}]
                    },
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["20260804", "direct"],
                            "metrics": [{"values": ["12"]}],
                        }
                    ]
                },
            }
        ]
    }
    analytics._get_report = MagicMock(return_value=raw)

    result = analytics.get_report(
        123,
        dimensions=["ga:date", "ga:source"],
        metrics=["ga:sessions"],
        start_date="7daysAgo",
        end_date="today",
    )

    analytics._get_report.assert_called_once_with(
        view_id=123,
        dimensions=["ga:date", "ga:source"],
        metrics=["ga:sessions"],
        start_date="7daysAgo",
        end_date="today",
    )
    assert result.to_dict("records") == [
        {
            "date": pd.Timestamp("2026-08-04"),
            "source": "direct",
            "sessions": 12,
        }
    ]


def test_get_all_reports_adds_view_metadata_and_concatenates():
    analytics = _analytics_without_init()
    analytics.list_views = MagicMock(
        return_value=[
            {
                "id": "1",
                "name": "Primary",
                "accountId": "account",
                "webPropertyId": "property",
            },
            {
                "id": "2",
                "name": "Secondary",
                "accountId": "account",
                "webPropertyId": "property",
            },
        ]
    )
    analytics.get_report = MagicMock(
        side_effect=[pd.DataFrame({"sessions": [1]}), pd.DataFrame({"sessions": [2]})]
    )

    result = analytics.get_all_reports()

    assert result["sessions"].tolist() == [1, 2]
    assert result["view_id"].tolist() == ["1", "2"]
    assert result["view_name"].tolist() == ["Primary", "Secondary"]
    assert result["view_accountId"].tolist() == ["account", "account"]
    assert result["view_webPropertyId"].tolist() == ["property", "property"]


def test_report_to_df_handles_empty_rows():
    raw = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:source"],
                    "metricHeader": {
                        "metricHeaderEntries": [{"name": "ga:sessions"}]
                    },
                },
                "data": {"rows": []},
            }
        ]
    }

    result = analyticsmod.Analytics.report_to_df(raw)

    assert list(result.columns) == ["source", "sessions"]
    assert result.empty
