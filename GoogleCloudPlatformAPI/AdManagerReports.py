"""Google Ad Manager Interactive Report API helpers.

This module provides a focused wrapper around the Google Ad Manager v1 Report
API. It complements the legacy SOAP-based ``ReportService`` in
``GoogleCloudPlatformAPI.AdManager`` and is intended for Interactive reports.
"""

import datetime
from collections.abc import Mapping, Sequence
from itertools import islice
from typing import Any, Dict, List, NoReturn, Optional, Union

import pandas as pd
from google.ads import admanager_v1
from google.api_core import exceptions as google_api_exceptions
from google.auth import exceptions as google_auth_exceptions
from google.auth.credentials import Credentials

from .AdManager import NETWORK_CODE
from .exceptions import AuthenticationError, ServiceError, TransportError


DEFAULT_REACH_DIMENSIONS = ("LINE_ITEM_ID", "LINE_ITEM_NAME")
DEFAULT_REACH_METRICS = ("REACH_IMPRESSIONS", "UNIQUE_VISITORS")
DEFAULT_REACH_DATE_RANGE = "LAST_30_DAYS"
DEFAULT_MAX_ROWS = 100_000
COUNTRY_DIMENSIONS = ("COUNTRY_CODE", "COUNTRY_ID", "COUNTRY_NAME")
AVERAGE_FREQUENCY_METRIC = "AVERAGE_IMPRESSIONS_PER_UNIQUE_VISITOR"

FILTER_OPERATIONS = {
    "in": "IN",
    "eq": "IN",
    "equals": "IN",
    "not_in": "NOT_IN",
    "ne": "NOT_IN",
    "not_equals": "NOT_IN",
    "contains": "CONTAINS",
    "not_contains": "NOT_CONTAINS",
    "lt": "LESS_THAN",
    "less_than": "LESS_THAN",
    "lte": "LESS_THAN_EQUALS",
    "less_than_equals": "LESS_THAN_EQUALS",
    "gt": "GREATER_THAN",
    "greater_than": "GREATER_THAN",
    "gte": "GREATER_THAN_EQUALS",
    "greater_than_equals": "GREATER_THAN_EQUALS",
    "between": "BETWEEN",
    "matches": "MATCHES",
    "not_matches": "NOT_MATCHES",
}

_SINGLE_VALUE_OPERATIONS = {
    "LESS_THAN",
    "LESS_THAN_EQUALS",
    "GREATER_THAN",
    "GREATER_THAN_EQUALS",
}

ReportEnumValue = Union[str, int]
FilterScalar = Union[str, int, float, bool, bytes]
FilterOperand = Union[FilterScalar, Sequence[FilterScalar]]
FilterRule = Union[FilterOperand, Mapping[str, FilterOperand]]
FilterSpec = Mapping[str, FilterRule]
FilterInput = Union[FilterSpec, Sequence[Any]]


class ReachReportService:
    """Create and run Reach reports with the Ad Manager v1 Report API.

    The service uses Application Default Credentials when ``credentials`` is
    omitted. A client can be injected for tests or custom transport settings.

    Parameters
    ----------
    network_code : str, optional
        Google Ad Manager network code. Defaults to the package ``NETWORK_CODE``.
    credentials : google.auth.credentials.Credentials, optional
        Credentials passed to ``ReportServiceClient``. When omitted, the Google
        client library resolves Application Default Credentials.
    client : google.ads.admanager_v1.ReportServiceClient, optional
        Pre-built report client. Primarily useful for tests and custom transports.

    Notes
    -----
    Reports created through the API are hidden in the Ad Manager UI by default.
    They remain addressable through the API and can be run again by resource name.
    """

    def __init__(
        self,
        network_code: str = NETWORK_CODE,
        credentials: Optional[Credentials] = None,
        client: Optional[Any] = None,
    ) -> None:
        self.network_code = str(network_code)
        try:
            if client is not None:
                self._client = client
            elif credentials is not None:
                self._client = admanager_v1.ReportServiceClient(credentials=credentials)
            else:
                self._client = admanager_v1.ReportServiceClient()
        except google_auth_exceptions.GoogleAuthError as exc:
            raise AuthenticationError(
                "Google Ad Manager authentication failed.",
                operation="admanager.client_init",
                details={"provider_error_type": type(exc).__name__},
            ) from exc

    @property
    def parent(self) -> str:
        """Return the network resource name.

        Returns
        -------
        str
            Resource name in ``networks/{network_code}`` format.
        """
        return f"networks/{self.network_code}"

    def report_name(self, report_id_or_name: Union[int, str]) -> str:
        """Return a canonical report resource name.

        Parameters
        ----------
        report_id_or_name : int or str
            Bare report ID or full ``networks/.../reports/...`` resource name.

        Returns
        -------
        str
            Canonical report resource name.
        """
        value = str(report_id_or_name)
        if value.startswith("networks/"):
            return value
        return f"{self.parent}/reports/{value}"

    @staticmethod
    def _coerce_enum(enum_type: Any, value: ReportEnumValue, label: str) -> Any:
        """Convert an enum name to the generated Ad Manager enum value."""
        if not isinstance(value, str):
            return value
        try:
            return enum_type[value]
        except KeyError as exc:
            raise ValueError(f"Unknown {label}: {value}") from exc

    @staticmethod
    def _enum_name(enum_type: Any, value: Any) -> str:
        """Return a stable generated enum member name."""
        name = getattr(value, "name", None)
        if name is not None:
            return str(name)
        try:
            return str(enum_type(value).name)
        except (TypeError, ValueError):
            return str(value)

    @staticmethod
    def _date_to_mapping(value: datetime.date) -> Dict[str, int]:
        """Convert a date to the Google type Date mapping shape."""
        return {"year": value.year, "month": value.month, "day": value.day}

    @staticmethod
    def _validate_pagination(page_size: int, max_rows: int) -> None:
        """Validate page and total-row bounds before remote work."""
        if (
            not isinstance(page_size, int)
            or isinstance(page_size, bool)
            or not 1 <= page_size <= 10_000
        ):
            raise ValueError("page_size must be between 1 and 10000")
        if (
            not isinstance(max_rows, int)
            or isinstance(max_rows, bool)
            or max_rows < 1
        ):
            raise ValueError("max_rows must be a positive integer")

    @staticmethod
    def _raise_google_error(operation: str, exc: Exception) -> NoReturn:
        """Normalize Google client failures to package-level exceptions."""
        details = {"provider_error_type": type(exc).__name__}
        if isinstance(
            exc,
            (
                google_auth_exceptions.GoogleAuthError,
                google_api_exceptions.Unauthenticated,
            ),
        ):
            raise AuthenticationError(
                "Google Ad Manager authentication failed.",
                operation=operation,
                details=details,
            ) from exc
        if isinstance(
            exc,
            (
                google_api_exceptions.DeadlineExceeded,
                google_api_exceptions.ServiceUnavailable,
                google_api_exceptions.RetryError,
                TimeoutError,
                ConnectionError,
                OSError,
            ),
        ):
            raise TransportError(
                "Google Ad Manager request could not complete.",
                operation=operation,
                details=details,
            ) from exc
        if isinstance(exc, google_api_exceptions.GoogleAPICallError):
            raise ServiceError(
                "Google Ad Manager rejected the request.",
                operation=operation,
                details=details,
            ) from exc
        raise exc

    @classmethod
    def _build_date_range(
        cls,
        relative_date_range: Optional[ReportEnumValue],
        start_date: Optional[datetime.date],
        end_date: Optional[datetime.date],
    ) -> admanager_v1.ReportDefinition.DateRange:
        """Build either a fixed or relative report date range."""
        if (start_date is None) != (end_date is None):
            raise ValueError("start_date and end_date must be provided together")

        if start_date is not None and end_date is not None:
            if relative_date_range is not None:
                raise ValueError(
                    "relative_date_range cannot be combined with start_date/end_date"
                )
            if start_date > end_date:
                raise ValueError("start_date must be on or before end_date")
            return admanager_v1.ReportDefinition.DateRange(
                fixed={
                    "start_date": cls._date_to_mapping(start_date),
                    "end_date": cls._date_to_mapping(end_date),
                }
            )

        relative_value = (
            DEFAULT_REACH_DATE_RANGE
            if relative_date_range is None
            else relative_date_range
        )
        relative = cls._coerce_enum(
            admanager_v1.ReportDefinition.DateRange.RelativeDateRange,
            relative_value,
            "relative date range",
        )
        return admanager_v1.ReportDefinition.DateRange(relative=relative)

    @staticmethod
    def _filter_values(value: Any) -> List[Any]:
        """Normalize a compact filter operand to a non-empty value list."""
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            values = list(value)
        else:
            values = [value]
        if not values:
            raise ValueError("filter values must not be empty")
        return values

    @staticmethod
    def _report_value(value: Any) -> admanager_v1.ReportValue:
        """Convert a Python scalar to the corresponding ReportValue."""
        if isinstance(value, bool):
            return admanager_v1.ReportValue(bool_value=value)
        if isinstance(value, int):
            return admanager_v1.ReportValue(int_value=value)
        if isinstance(value, float):
            return admanager_v1.ReportValue(double_value=value)
        if isinstance(value, str):
            return admanager_v1.ReportValue(string_value=value)
        if isinstance(value, (bytes, bytearray)):
            return admanager_v1.ReportValue(bytes_value=bytes(value))
        raise TypeError(
            "filter values must be str, int, float, bool, or bytes; "
            f"got {type(value).__name__}"
        )

    @classmethod
    def _filter_field(cls, field_name: str) -> admanager_v1.ReportDefinition.Field:
        """Resolve a compact field name to a dimension or metric field."""
        if not isinstance(field_name, str):
            raise TypeError("filter field names must be strings")

        try:
            dimension = cls._coerce_enum(
                admanager_v1.ReportDefinition.Dimension,
                field_name,
                "dimension",
            )
        except ValueError:
            try:
                metric = cls._coerce_enum(
                    admanager_v1.ReportDefinition.Metric,
                    field_name,
                    "metric",
                )
            except ValueError as exc:
                raise ValueError(f"Unknown report filter field: {field_name}") from exc
            return admanager_v1.ReportDefinition.Field(metric=metric)
        return admanager_v1.ReportDefinition.Field(dimension=dimension)

    @classmethod
    def _filter_operation_and_values(
        cls,
        rule: Any,
    ) -> tuple[Any, List[admanager_v1.ReportValue]]:
        """Resolve a compact rule to an API operation and ReportValues."""
        if isinstance(rule, Mapping):
            if len(rule) != 1:
                raise ValueError(
                    "operator filter mappings must contain exactly one operation"
                )
            operation_key, raw_values = next(iter(rule.items()))
            if not isinstance(operation_key, str):
                raise TypeError("filter operation names must be strings")
            normalized_key = operation_key.strip().lower().replace("-", "_")
            try:
                operation_name = FILTER_OPERATIONS[normalized_key]
            except KeyError as exc:
                raise ValueError(f"Unknown filter operation: {operation_key}") from exc
        else:
            operation_name = "IN"
            raw_values = rule

        values = cls._filter_values(raw_values)
        if operation_name in _SINGLE_VALUE_OPERATIONS and len(values) != 1:
            raise ValueError(f"{operation_name} requires exactly one filter value")
        if operation_name == "BETWEEN" and len(values) != 2:
            raise ValueError("BETWEEN requires exactly two filter values")

        operation = admanager_v1.ReportDefinition.Filter.Operation[operation_name]
        return operation, [cls._report_value(value) for value in values]

    @classmethod
    def _compact_filter(
        cls,
        field_name: str,
        rule: Any,
    ) -> admanager_v1.ReportDefinition.Filter:
        """Build one native Ad Manager filter from a compact filter rule."""
        operation, values = cls._filter_operation_and_values(rule)
        return admanager_v1.ReportDefinition.Filter(
            field_filter=admanager_v1.ReportDefinition.Filter.FieldFilter(
                field=cls._filter_field(field_name),
                operation=operation,
                values=values,
            )
        )

    @classmethod
    def _normalize_filters(cls, filters: Optional[FilterInput]) -> List[Any]:
        """Expand compact filters while preserving raw Google filters."""
        if filters is None:
            return []
        if not isinstance(filters, Mapping):
            return list(filters)

        compact_filters = [
            cls._compact_filter(field_name, rule)
            for field_name, rule in filters.items()
        ]
        if len(compact_filters) <= 1:
            return compact_filters
        return [
            admanager_v1.ReportDefinition.Filter(
                and_filter=admanager_v1.ReportDefinition.Filter.FilterList(
                    filters=compact_filters
                )
            )
        ]

    @classmethod
    def build_report(
        cls,
        display_name: str,
        dimensions: Sequence[ReportEnumValue] = DEFAULT_REACH_DIMENSIONS,
        metrics: Sequence[ReportEnumValue] = DEFAULT_REACH_METRICS,
        relative_date_range: Optional[ReportEnumValue] = None,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        filters: Optional[FilterInput] = None,
    ) -> admanager_v1.Report:
        """Build a Reach report resource without sending it to Google.

        Parameters
        ----------
        display_name : str
            Display name for the report.
        dimensions : sequence[str or int], optional
            Report dimensions. String values use Ad Manager API enum names.
        metrics : sequence[str or int], optional
            Reach metrics. Defaults to reach impressions and unique visitors.
        relative_date_range : str or int, optional
            Relative date range enum name, such as ``"LAST_30_DAYS"``. If no
            date arguments are supplied, ``LAST_30_DAYS`` is used.
        start_date : datetime.date, optional
            Start of an explicit fixed date range, inclusive.
        end_date : datetime.date, optional
            End of an explicit fixed date range, inclusive.
        filters : mapping or sequence, optional
            Compact mapping of dimension or metric names to scalar/list values,
            or to one-operation mappings such as ``{"not_in": [1, 2]}``.
            A sequence preserves raw generated Google filter messages/mappings.

        Returns
        -------
        google.ads.admanager_v1.Report
            Unpersisted Reach report resource.

        Raises
        ------
        ValueError
            If date arguments, enums, filters, or Reach combinations are invalid.
        """
        if not display_name.strip():
            raise ValueError("display_name must not be empty")
        if not metrics:
            raise ValueError("metrics must contain at least one Reach metric")

        dimension_values = [
            cls._coerce_enum(
                admanager_v1.ReportDefinition.Dimension, value, "dimension"
            )
            for value in dimensions
        ]
        metric_values = [
            cls._coerce_enum(admanager_v1.ReportDefinition.Metric, value, "metric")
            for value in metrics
        ]

        dimension_names = {
            cls._enum_name(admanager_v1.ReportDefinition.Dimension, value)
            for value in dimension_values
        }
        metric_names = {
            cls._enum_name(admanager_v1.ReportDefinition.Metric, value)
            for value in metric_values
        }
        if AVERAGE_FREQUENCY_METRIC in metric_names and not (
            dimension_names & set(COUNTRY_DIMENSIONS)
        ):
            raise ValueError(f"{AVERAGE_FREQUENCY_METRIC} requires a country dimension")

        report_definition = admanager_v1.ReportDefinition(
            dimensions=dimension_values,
            metrics=metric_values,
            filters=cls._normalize_filters(filters),
            date_range=cls._build_date_range(
                relative_date_range=relative_date_range,
                start_date=start_date,
                end_date=end_date,
            ),
            report_type=admanager_v1.ReportDefinition.ReportType.REACH,
        )
        return admanager_v1.Report(
            display_name=display_name,
            report_definition=report_definition,
        )

    def create_report(
        self,
        display_name: str,
        dimensions: Sequence[ReportEnumValue] = DEFAULT_REACH_DIMENSIONS,
        metrics: Sequence[ReportEnumValue] = DEFAULT_REACH_METRICS,
        relative_date_range: Optional[ReportEnumValue] = None,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        filters: Optional[FilterInput] = None,
    ) -> admanager_v1.Report:
        """Create a hidden Reach report in Google Ad Manager.

        Parameters
        ----------
        display_name : str
            Display name for the report.
        dimensions : sequence[str or int], optional
            Report dimensions. Defaults to line item ID and name.
        metrics : sequence[str or int], optional
            Reach metrics. Defaults to reach impressions and unique visitors.
        relative_date_range : str or int, optional
            Relative date range enum name. Defaults to ``LAST_30_DAYS`` when no
            fixed date range is provided.
        start_date : datetime.date, optional
            Start of an explicit fixed date range, inclusive.
        end_date : datetime.date, optional
            End of an explicit fixed date range, inclusive.
        filters : mapping or sequence, optional
            Compact filter mapping or raw Google filters.

        Returns
        -------
        google.ads.admanager_v1.Report
            Created report resource, including its resource name and report ID.
        """
        report = self.build_report(
            display_name=display_name,
            dimensions=dimensions,
            metrics=metrics,
            relative_date_range=relative_date_range,
            start_date=start_date,
            end_date=end_date,
            filters=filters,
        )
        request = admanager_v1.CreateReportRequest(parent=self.parent, report=report)
        try:
            return self._client.create_report(request=request)
        except Exception as exc:
            self._raise_google_error("admanager.create_report", exc)

    def get_report(self, report_id_or_name: Union[int, str]) -> admanager_v1.Report:
        """Get an existing Reach or Interactive report definition.

        Parameters
        ----------
        report_id_or_name : int or str
            Bare report ID or full report resource name.

        Returns
        -------
        google.ads.admanager_v1.Report
            Existing report resource.
        """
        request = admanager_v1.GetReportRequest(
            name=self.report_name(report_id_or_name)
        )
        try:
            return self._client.get_report(request=request)
        except Exception as exc:
            self._raise_google_error("admanager.get_report", exc)

    def run_report(
        self,
        report_id_or_name: Union[int, str],
        timeout: Optional[float] = None,
    ) -> str:
        """Run an existing report and return the completed result resource name.

        Parameters
        ----------
        report_id_or_name : int or str
            Bare report ID or full report resource name.
        timeout : float, optional
            Maximum seconds to wait for the long-running operation. ``None`` uses
            the client library default.

        Returns
        -------
        str
            Report result resource name suitable for ``fetch_rows``.
        """
        request = admanager_v1.RunReportRequest(
            name=self.report_name(report_id_or_name)
        )
        try:
            operation = self._client.run_report(request=request)
            if timeout is None:
                response = operation.result()
            else:
                response = operation.result(timeout=timeout)
        except Exception as exc:
            self._raise_google_error("admanager.run_report", exc)
        if response is None:
            raise ServiceError(
                "Ad Manager report operation completed without a result.",
                operation="admanager.run_report",
            )
        return str(response.report_result)

    def fetch_rows(
        self,
        result_name: str,
        page_size: int = 10_000,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> List[Any]:
        """Fetch bounded rows for a completed report result.

        Parameters
        ----------
        result_name : str
            Full report result resource name returned by ``run_report``.
        page_size : int, optional
            Requested rows per page. Ad Manager allows at most 10,000.
        max_rows : int, optional
            Maximum total rows to materialize. Defaults to 100,000. If the
            result exceeds this bound, the method raises instead of truncating.

        Returns
        -------
        list
            Materialized ``ReportDataTable.Row`` messages.

        Raises
        ------
        ValueError
            If pagination bounds are invalid.
        ServiceError
            If the report result exceeds ``max_rows``.
        """
        self._validate_pagination(page_size=page_size, max_rows=max_rows)
        request = admanager_v1.FetchReportResultRowsRequest(
            name=result_name,
            page_size=min(page_size, max_rows + 1),
        )
        try:
            pager = self._client.fetch_report_result_rows(request=request)
            rows = list(islice(pager, max_rows + 1))
        except Exception as exc:
            self._raise_google_error("admanager.fetch_report_rows", exc)
        if len(rows) > max_rows:
            raise ServiceError(
                "Ad Manager report result exceeds the configured row limit.",
                operation="admanager.fetch_report_rows",
                details={"max_rows": max_rows},
            )
        return rows

    @staticmethod
    def _report_value_to_python(value: Any) -> Any:
        """Convert a ReportValue or compatible mapping to a Python value."""
        if isinstance(value, Mapping):
            data = dict(value)
            if not data:
                return None
            _, raw_value = next(iter(data.items()))
            if isinstance(raw_value, Mapping) and "values" in raw_value:
                return list(raw_value["values"])
            return raw_value

        field_name = type(value).pb(value).WhichOneof("value")
        if field_name is None:
            return None
        raw_value = getattr(value, field_name)
        if field_name.endswith("_list_value"):
            return list(raw_value.values)
        return raw_value

    @classmethod
    def rows_to_dataframe(
        cls,
        rows: Sequence[Any],
        dimensions: Sequence[Any],
        metrics: Sequence[Any],
    ) -> pd.DataFrame:
        """Convert report result rows to a typed-friendly DataFrame.

        Parameters
        ----------
        rows : sequence
            ``ReportDataTable.Row`` messages returned by the Report API.
        dimensions : sequence
            Dimensions from the report definition, in report order.
        metrics : sequence
            Metrics from the report definition, in report order.

        Returns
        -------
        pandas.DataFrame
            DataFrame with lowercase API enum names as columns.

        Raises
        ------
        ValueError
            If a row contains multiple metric groups. This wrapper intentionally
            targets Reach reports without comparison date ranges or time-period
            column splits so no result data is silently discarded.
        """
        dimension_names = [
            cls._enum_name(admanager_v1.ReportDefinition.Dimension, value).lower()
            for value in dimensions
        ]
        metric_names = [
            cls._enum_name(admanager_v1.ReportDefinition.Metric, value).lower()
            for value in metrics
        ]
        columns = dimension_names + metric_names
        records: List[Dict[str, Any]] = []

        for row in rows:
            metric_groups = list(row.metric_value_groups)
            if len(metric_groups) > 1:
                raise ValueError(
                    "multiple metric value groups are not supported; run a report "
                    "without comparison date ranges or time-period column splits"
                )
            dimension_values = list(row.dimension_values)
            primary_values = (
                list(metric_groups[0].primary_values) if metric_groups else []
            )
            record: Dict[str, Any] = {}
            for index, name in enumerate(dimension_names):
                record[name] = (
                    cls._report_value_to_python(dimension_values[index])
                    if index < len(dimension_values)
                    else None
                )
            for index, name in enumerate(metric_names):
                record[name] = (
                    cls._report_value_to_python(primary_values[index])
                    if index < len(primary_values)
                    else None
                )
            records.append(record)

        return pd.DataFrame.from_records(records, columns=columns)

    def get_report_dataframe(
        self,
        report_id_or_name: Union[int, str],
        timeout: Optional[float] = None,
        page_size: int = 10_000,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> pd.DataFrame:
        """Run an existing report and return its primary values as a DataFrame.

        Parameters
        ----------
        report_id_or_name : int or str
            Bare report ID or full report resource name.
        timeout : float, optional
            Maximum seconds to wait for report generation.
        page_size : int, optional
            Rows requested per result page. Maximum 10,000.
        max_rows : int, optional
            Maximum total rows to materialize. Defaults to 100,000.

        Returns
        -------
        pandas.DataFrame
            Report data with dimensions followed by metrics.
        """
        self._validate_pagination(page_size=page_size, max_rows=max_rows)
        report = self.get_report(report_id_or_name)
        result_name = self.run_report(report.name, timeout=timeout)
        rows = self.fetch_rows(
            result_name=result_name,
            page_size=page_size,
            max_rows=max_rows,
        )
        return self.rows_to_dataframe(
            rows=rows,
            dimensions=report.report_definition.dimensions,
            metrics=report.report_definition.metrics,
        )

    def create_and_get_dataframe(
        self,
        display_name: str,
        dimensions: Sequence[ReportEnumValue] = DEFAULT_REACH_DIMENSIONS,
        metrics: Sequence[ReportEnumValue] = DEFAULT_REACH_METRICS,
        relative_date_range: Optional[ReportEnumValue] = None,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        filters: Optional[FilterInput] = None,
        timeout: Optional[float] = None,
        page_size: int = 10_000,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> pd.DataFrame:
        """Create a Reach report, run it, and return the result DataFrame.

        Parameters
        ----------
        display_name : str
            Display name for the new hidden report resource.
        dimensions : sequence[str or int], optional
            Report dimensions.
        metrics : sequence[str or int], optional
            Reach metrics.
        relative_date_range : str or int, optional
            Relative date range enum name. Defaults to ``LAST_30_DAYS``.
        start_date : datetime.date, optional
            Start of an explicit fixed date range, inclusive.
        end_date : datetime.date, optional
            End of an explicit fixed date range, inclusive.
        filters : mapping or sequence, optional
            Compact filter mapping or raw Google filters.
        timeout : float, optional
            Maximum seconds to wait for report generation.
        page_size : int, optional
            Rows requested per result page. Maximum 10,000.
        max_rows : int, optional
            Maximum total rows to materialize. Defaults to 100,000.

        Returns
        -------
        pandas.DataFrame
            Report data with dimensions followed by Reach metrics.
        """
        self._validate_pagination(page_size=page_size, max_rows=max_rows)
        report = self.create_report(
            display_name=display_name,
            dimensions=dimensions,
            metrics=metrics,
            relative_date_range=relative_date_range,
            start_date=start_date,
            end_date=end_date,
            filters=filters,
        )
        result_name = self.run_report(report.name, timeout=timeout)
        rows = self.fetch_rows(
            result_name=result_name,
            page_size=page_size,
            max_rows=max_rows,
        )
        return self.rows_to_dataframe(
            rows=rows,
            dimensions=report.report_definition.dimensions,
            metrics=report.report_definition.metrics,
        )
