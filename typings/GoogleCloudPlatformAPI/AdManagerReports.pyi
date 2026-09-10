"""Type stubs for Google Ad Manager Interactive Report API helpers."""

import datetime
from collections.abc import Mapping, Sequence
from typing import Any, List, Optional, Union

import pandas as pd
from google.ads import admanager_v1
from google.auth.credentials import Credentials

DEFAULT_REACH_DIMENSIONS: tuple[str, str]
DEFAULT_REACH_METRICS: tuple[str, str]
DEFAULT_REACH_DATE_RANGE: str
COUNTRY_DIMENSIONS: tuple[str, str, str]
AVERAGE_FREQUENCY_METRIC: str
FILTER_OPERATIONS: dict[str, str]
ReportEnumValue = Union[str, int]
FilterScalar = Union[str, int, float, bool, bytes]
FilterOperand = Union[FilterScalar, Sequence[FilterScalar]]
FilterRule = Union[FilterOperand, Mapping[str, FilterOperand]]
FilterSpec = Mapping[str, FilterRule]
FilterInput = Union[FilterSpec, Sequence[Any]]

class ReachReportService:
    network_code: str
    def __init__(
        self,
        network_code: str = ...,
        credentials: Optional[Credentials] = ...,
        client: Optional[Any] = ...,
    ) -> None: ...
    @property
    def parent(self) -> str: ...
    def report_name(self, report_id_or_name: Union[int, str]) -> str: ...
    @classmethod
    def build_report(
        cls,
        display_name: str,
        dimensions: Sequence[ReportEnumValue] = ...,
        metrics: Sequence[ReportEnumValue] = ...,
        relative_date_range: Optional[ReportEnumValue] = ...,
        start_date: Optional[datetime.date] = ...,
        end_date: Optional[datetime.date] = ...,
        filters: Optional[FilterInput] = ...,
    ) -> admanager_v1.Report: ...
    def create_report(
        self,
        display_name: str,
        dimensions: Sequence[ReportEnumValue] = ...,
        metrics: Sequence[ReportEnumValue] = ...,
        relative_date_range: Optional[ReportEnumValue] = ...,
        start_date: Optional[datetime.date] = ...,
        end_date: Optional[datetime.date] = ...,
        filters: Optional[FilterInput] = ...,
    ) -> admanager_v1.Report: ...
    def get_report(self, report_id_or_name: Union[int, str]) -> admanager_v1.Report: ...
    def run_report(
        self,
        report_id_or_name: Union[int, str],
        timeout: Optional[float] = ...,
    ) -> str: ...
    def fetch_rows(self, result_name: str, page_size: int = ...) -> List[Any]: ...
    @classmethod
    def rows_to_dataframe(
        cls,
        rows: Sequence[Any],
        dimensions: Sequence[Any],
        metrics: Sequence[Any],
    ) -> pd.DataFrame: ...
    def get_report_dataframe(
        self,
        report_id_or_name: Union[int, str],
        timeout: Optional[float] = ...,
        page_size: int = ...,
    ) -> pd.DataFrame: ...
    def create_and_get_dataframe(
        self,
        display_name: str,
        dimensions: Sequence[ReportEnumValue] = ...,
        metrics: Sequence[ReportEnumValue] = ...,
        relative_date_range: Optional[ReportEnumValue] = ...,
        start_date: Optional[datetime.date] = ...,
        end_date: Optional[datetime.date] = ...,
        filters: Optional[FilterInput] = ...,
        timeout: Optional[float] = ...,
        page_size: int = ...,
    ) -> pd.DataFrame: ...
