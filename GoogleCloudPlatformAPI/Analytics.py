"""Wrapper for Google Analytics Reporting API."""

from typing import List, Optional
import logging
import os
import pandas as pd
from googleapiclient.discovery import build
import datetime
from .Oauth import ServiceAccount


class Analytics:
    """High level helper for the Google Analytics Reporting API."""

    SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

    def __init__(self, credentials: Optional[str] = None) -> None:
        """Initialise Analytics API clients.

        Parameters
        ----------
        credentials : str, optional
            Path to a service account JSON file. If ``None``, the value from
            ``GOOGLE_APPLICATION_CREDENTIALS`` is used.
        """
        logging.debug("Analytics::__init__")
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        service_account_credentials = ServiceAccount.from_service_account_file(
            credentials=credentials, scopes=self.SCOPES
        )
        self.__reporting = build("analyticsreporting", "v4", credentials=service_account_credentials)
        self.__management = build("analytics", "v3", credentials=service_account_credentials)

    def list_views(self) -> List[dict]:
        """Return all accessible Analytics views."""
        profiles = self.__management.management().profiles().list(accountId="~all", webPropertyId="~all").execute()
        return profiles.get("items", [])

    def __get_report(
        self,
        view_id: int,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
    ) -> dict:
        """Fetch a report from the API."""
        logging.debug("Analytics::__get_report")
        if isinstance(start_date, datetime.date):
            start_date = start_date.strftime("%Y-%m-%d")
            logging.debug(f"__get_report::start_date::{start_date}")
        if isinstance(end_date, datetime.date):
            end_date = end_date.strftime("%Y-%m-%d")
            logging.debug(f"__get_report::end_date::{end_date}")
        return self.__reporting.reports().batchGet(
            body={
                "reportRequests": [
                    {
                        "viewId": str(view_id),
                        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
                        "metrics": [{"expression": m} for m in metrics],
                        "dimensions": [{"name": d} for d in dimensions],
                        "pageSize": 100000,
                    }
                ]
            }
        ).execute()

    def get_report(
        self,
        view_id: int,
        dimensions: List[str] = ["ga:source", "ga:medium"],
        metrics: List[str] = ["ga:sessions"],
        start_date: str = "30daysAgo",
        end_date: str = "yesterday",
    ) -> pd.DataFrame:
        """Return a report for a specific view as a DataFrame."""
        logging.debug(f"Analytics::get_report::{view_id}")
        results = self.__get_report(
            view_id=view_id,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
        )
        return Analytics.report_to_df(results)

    def get_all_reports(
        self,
        dimensions: List[str] = ["ga:source", "ga:medium"],
        metrics: List[str] = ["ga:sessions"],
        start_date: str = "30daysAgo",
        end_date: str = "yesterday",
    ) -> pd.DataFrame:
        """Fetch reports for all available views and combine them."""
        views = self.list_views()
        df_list: List[pd.DataFrame] = []
        for view in views:
            df_view = self.get_report(
                view_id=view["id"],
                dimensions=dimensions,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
            )
            df_view["view_id"] = view["id"]
            df_view["view_name"] = view["name"]
            df_view["view_accountId"] = view["accountId"]
            df_view["view_webPropertyId"] = view["webPropertyId"]
            df_list.append(df_view)
        return pd.concat(df_list)

    @staticmethod
    def report_to_df(analytics_report: dict) -> pd.DataFrame:
        """Convert an Analytics API response to a DataFrame."""
        report = analytics_report["reports"][0]
        dimensions: List[str] = [d.replace("ga:", "") for d in report["columnHeader"]["dimensions"]]
        metrics: List[str] = [
            m["name"].replace("ga:", "") for m in report["columnHeader"]["metricHeader"]["metricHeaderEntries"]
        ]
        headers: List[str] = list(dimensions) + list(metrics)

        data_rows = report["data"].get("rows", [])
        data: List[List[str]] = []
        for row in data_rows:
            data.append([*row["dimensions"], *row["metrics"][0]["values"]])
        df = pd.DataFrame(data=data, columns=pd.Index(headers))
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        for metric in metrics:
            df[metric] = pd.to_numeric(df[metric], errors="coerce")

        return df
