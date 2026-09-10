"""Tests for the Google Ad Manager v1 Reach report wrapper."""

import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
from google.ads import admanager_v1

from GoogleCloudPlatformAPI.AdManagerReports import ReachReportService


def test_build_report_uses_reach_defaults():
    """Build a REACH report with safe line-item defaults."""
    report = ReachReportService.build_report("30 day reach")

    definition = report.report_definition
    assert definition.report_type == admanager_v1.ReportDefinition.ReportType.REACH
    assert list(definition.dimensions) == [
        admanager_v1.ReportDefinition.Dimension.LINE_ITEM_ID,
        admanager_v1.ReportDefinition.Dimension.LINE_ITEM_NAME,
    ]
    assert list(definition.metrics) == [
        admanager_v1.ReportDefinition.Metric.REACH_IMPRESSIONS,
        admanager_v1.ReportDefinition.Metric.UNIQUE_VISITORS,
    ]
    assert (
        definition.date_range.relative
        == admanager_v1.ReportDefinition.DateRange.RelativeDateRange.LAST_30_DAYS
    )


def test_build_report_supports_fixed_date_range():
    """Build a Reach report with an explicit inclusive date range."""
    report = ReachReportService.build_report(
        "fixed reach",
        start_date=datetime.date(2026, 8, 1),
        end_date=datetime.date(2026, 8, 31),
    )

    fixed = report.report_definition.date_range.fixed
    assert (fixed.start_date.year, fixed.start_date.month, fixed.start_date.day) == (
        2026,
        8,
        1,
    )
    assert (fixed.end_date.year, fixed.end_date.month, fixed.end_date.day) == (
        2026,
        8,
        31,
    )


def test_build_report_rejects_inconsistent_dates():
    """Reject incomplete, reversed, or conflicting date arguments."""
    with pytest.raises(ValueError, match="provided together"):
        ReachReportService.build_report("reach", start_date=datetime.date(2026, 8, 1))

    with pytest.raises(ValueError, match="on or before"):
        ReachReportService.build_report(
            "reach",
            start_date=datetime.date(2026, 8, 31),
            end_date=datetime.date(2026, 8, 1),
        )

    with pytest.raises(ValueError, match="cannot be combined"):
        ReachReportService.build_report(
            "reach",
            relative_date_range="LAST_7_DAYS",
            start_date=datetime.date(2026, 8, 1),
            end_date=datetime.date(2026, 8, 31),
        )


def test_average_frequency_requires_country_dimension():
    """Surface the Ad Manager average-frequency compatibility rule early."""
    with pytest.raises(ValueError, match="requires a country dimension"):
        ReachReportService.build_report(
            "frequency",
            dimensions=("LINE_ITEM_ID",),
            metrics=("AVERAGE_IMPRESSIONS_PER_UNIQUE_VISITOR",),
        )

    report = ReachReportService.build_report(
        "frequency by country",
        dimensions=("LINE_ITEM_ID", "COUNTRY_NAME"),
        metrics=("AVERAGE_IMPRESSIONS_PER_UNIQUE_VISITOR",),
    )
    assert report.report_definition.metrics[0] == (
        admanager_v1.ReportDefinition.Metric.AVERAGE_IMPRESSIONS_PER_UNIQUE_VISITOR
    )


def test_compact_filters_convert_scalars_and_lists_to_in_filters():
    """Turn compact scalar/list filters into native IN filters joined by AND."""
    report = ReachReportService.build_report(
        "filtered reach",
        filters={
            "LINE_ITEM_ID": [101, 202],
            "COUNTRY_NAME": "France",
        },
    )

    filters = list(report.report_definition.filters)
    assert len(filters) == 1
    native_filters = list(filters[0].and_filter.filters)
    assert len(native_filters) == 2

    line_items = native_filters[0].field_filter
    assert line_items.field.dimension == (
        admanager_v1.ReportDefinition.Dimension.LINE_ITEM_ID
    )
    assert line_items.operation == admanager_v1.ReportDefinition.Filter.Operation.IN
    assert [value.int_value for value in line_items.values] == [101, 202]

    country = native_filters[1].field_filter
    assert country.field.dimension == (
        admanager_v1.ReportDefinition.Dimension.COUNTRY_NAME
    )
    assert country.operation == admanager_v1.ReportDefinition.Filter.Operation.IN
    assert [value.string_value for value in country.values] == ["France"]


def test_compact_filters_support_operators_and_metric_fields():
    """Support concise operators and resolve metric fields when needed."""
    report = ReachReportService.build_report(
        "advanced filters",
        filters={
            "COUNTRY_NAME": {"not_in": ["Germany", "Spain"]},
            "LINE_ITEM_NAME": {"contains": "Brand"},
            "UNIQUE_VISITORS": {"gte": 100},
        },
    )

    native_filters = list(report.report_definition.filters[0].and_filter.filters)
    country = native_filters[0].field_filter
    line_item = native_filters[1].field_filter
    visitors = native_filters[2].field_filter

    assert country.operation == admanager_v1.ReportDefinition.Filter.Operation.NOT_IN
    assert [value.string_value for value in country.values] == ["Germany", "Spain"]
    assert (
        line_item.operation
        == admanager_v1.ReportDefinition.Filter.Operation.CONTAINS
    )
    assert [value.string_value for value in line_item.values] == ["Brand"]
    assert visitors.field.metric == admanager_v1.ReportDefinition.Metric.UNIQUE_VISITORS
    assert (
        visitors.operation
        == admanager_v1.ReportDefinition.Filter.Operation.GREATER_THAN_EQUALS
    )
    assert visitors.values[0].int_value == 100


def test_compact_filters_validate_fields_operators_and_cardinality():
    """Reject malformed compact filters before making an API request."""
    with pytest.raises(ValueError, match="Unknown report filter field"):
        ReachReportService.build_report("reach", filters={"NOT_A_FIELD": 1})

    with pytest.raises(ValueError, match="Unknown filter operation"):
        ReachReportService.build_report(
            "reach", filters={"LINE_ITEM_ID": {"approximately": 1}}
        )

    with pytest.raises(ValueError, match="exactly two"):
        ReachReportService.build_report(
            "reach", filters={"LINE_ITEM_ID": {"between": [1]}}
        )

    with pytest.raises(ValueError, match="must not be empty"):
        ReachReportService.build_report("reach", filters={"LINE_ITEM_ID": []})


def test_raw_google_filters_remain_supported():
    """Keep generated Google filter messages as an advanced escape hatch."""
    raw_filter = admanager_v1.ReportDefinition.Filter(
        field_filter=admanager_v1.ReportDefinition.Filter.FieldFilter(
            field=admanager_v1.ReportDefinition.Field(
                dimension=admanager_v1.ReportDefinition.Dimension.LINE_ITEM_ID
            ),
            operation=admanager_v1.ReportDefinition.Filter.Operation.IN,
            values=[admanager_v1.ReportValue(int_value=101)],
        )
    )

    report = ReachReportService.build_report("raw filter", filters=(raw_filter,))

    assert report.report_definition.filters[0] == raw_filter


def test_create_report_uses_network_parent():
    """Create the report under the configured network resource."""
    client = MagicMock()
    client.create_report.return_value = admanager_v1.Report(
        name="networks/123/reports/456"
    )
    service = ReachReportService(network_code="123", client=client)

    created = service.create_report("reach")

    request = client.create_report.call_args.kwargs["request"]
    assert request.parent == "networks/123"
    assert request.report.report_definition.report_type == (
        admanager_v1.ReportDefinition.ReportType.REACH
    )
    assert created.name == "networks/123/reports/456"


def test_run_report_waits_for_operation_and_returns_result_name():
    """Run an Interactive report through the long-running operation API."""
    client = MagicMock()
    operation = MagicMock()
    operation.result.return_value = admanager_v1.RunReportResponse(
        report_result="networks/123/reports/456/results/789"
    )
    client.run_report.return_value = operation
    service = ReachReportService(network_code="123", client=client)

    result_name = service.run_report(456, timeout=30)

    request = client.run_report.call_args.kwargs["request"]
    assert request.name == "networks/123/reports/456"
    operation.result.assert_called_once_with(timeout=30)
    assert result_name == "networks/123/reports/456/results/789"


def test_fetch_rows_enforces_api_page_size_limit():
    """Keep result pagination within the API's documented bounds."""
    service = ReachReportService(network_code="123", client=MagicMock())

    with pytest.raises(ValueError, match="between 1 and 10000"):
        service.fetch_rows("result", page_size=10_001)


def test_rows_to_dataframe_preserves_definition_order_and_value_types():
    """Convert v1 ReportValue messages into predictable DataFrame columns."""
    row = admanager_v1.ReportDataTable.Row(
        dimension_values=[
            admanager_v1.ReportValue(int_value=101),
            admanager_v1.ReportValue(string_value="Line item A"),
        ],
        metric_value_groups=[
            admanager_v1.ReportDataTable.MetricValueGroup(
                primary_values=[
                    admanager_v1.ReportValue(int_value=1_000),
                    admanager_v1.ReportValue(string_value="-"),
                ]
            )
        ],
    )

    df = ReachReportService.rows_to_dataframe(
        rows=[row],
        dimensions=(
            admanager_v1.ReportDefinition.Dimension.LINE_ITEM_ID,
            admanager_v1.ReportDefinition.Dimension.LINE_ITEM_NAME,
        ),
        metrics=(
            admanager_v1.ReportDefinition.Metric.REACH_IMPRESSIONS,
            admanager_v1.ReportDefinition.Metric.UNIQUE_VISITORS,
        ),
    )

    expected = pd.DataFrame(
        {
            "line_item_id": [101],
            "line_item_name": ["Line item A"],
            "reach_impressions": [1_000],
            "unique_visitors": ["-"],
        }
    )
    pd.testing.assert_frame_equal(df, expected)


def test_rows_to_dataframe_rejects_comparison_metric_groups():
    """Never silently drop secondary metric groups from existing reports."""
    row = admanager_v1.ReportDataTable.Row(
        metric_value_groups=[
            admanager_v1.ReportDataTable.MetricValueGroup(
                primary_values=[admanager_v1.ReportValue(int_value=10)]
            ),
            admanager_v1.ReportDataTable.MetricValueGroup(
                primary_values=[admanager_v1.ReportValue(int_value=20)]
            ),
        ]
    )

    with pytest.raises(ValueError, match="multiple metric value groups"):
        ReachReportService.rows_to_dataframe(
            rows=[row],
            dimensions=(),
            metrics=(admanager_v1.ReportDefinition.Metric.UNIQUE_VISITORS,),
        )


def test_get_report_dataframe_runs_and_fetches_existing_report():
    """Fetch a saved report definition before converting its result rows."""
    client = MagicMock()
    client.get_report.return_value = ReachReportService.build_report("reach")
    client.get_report.return_value.name = "networks/123/reports/456"
    operation = MagicMock()
    operation.result.return_value = admanager_v1.RunReportResponse(
        report_result="networks/123/reports/456/results/789"
    )
    client.run_report.return_value = operation
    client.fetch_report_result_rows.return_value = [
        admanager_v1.ReportDataTable.Row(
            dimension_values=[
                admanager_v1.ReportValue(int_value=101),
                admanager_v1.ReportValue(string_value="Line item A"),
            ],
            metric_value_groups=[
                admanager_v1.ReportDataTable.MetricValueGroup(
                    primary_values=[
                        admanager_v1.ReportValue(int_value=1_000),
                        admanager_v1.ReportValue(int_value=500),
                    ]
                )
            ],
        )
    ]
    service = ReachReportService(network_code="123", client=client)

    df = service.get_report_dataframe(456)

    assert list(df.columns) == [
        "line_item_id",
        "line_item_name",
        "reach_impressions",
        "unique_visitors",
    ]
    assert df.iloc[0].to_dict() == {
        "line_item_id": 101,
        "line_item_name": "Line item A",
        "reach_impressions": 1_000,
        "unique_visitors": 500,
    }
