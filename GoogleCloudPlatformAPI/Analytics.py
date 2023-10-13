from typing import List, Optional
import logging
import os
import pandas as pd
from googleapiclient.discovery import build
import datetime
from .ServiceAccount import ServiceAccount


class Analytics():
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

    def __init__(self, credentials: Optional[str] = None):
        logging.info(f"Analytics::__init__")
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        service_account_credentials = ServiceAccount.from_service_account_file(
            credentials=credentials, scopes=self.SCOPES
        )
        self.__reporting = build('analyticsreporting', 'v4',
                                 credentials=service_account_credentials)
        self.__management = build('analytics', 'v3',
                                  credentials=service_account_credentials)

    def list_views(self):
        profiles = self.__management.management().profiles().list(
            accountId='~all',
            webPropertyId='~all').execute()
        return profiles.get('items', [])

    def __get_report(self,
                     view_id: int,
                     dimensions: List[str],
                     metrics: List[str],
                     start_date: str,
                     end_date: str):
        logging.info(f"Analytics::__get_report")
        if isinstance(start_date, datetime.date):
            start_date = start_date.strftime('%Y-%m-%d')
            logging.info(f'__get_report::start_date::{start_date}')
        if isinstance(end_date, datetime.date):
            end_date = end_date.strftime('%Y-%m-%d')
            logging.info(f'__get_report::end_date::{end_date}')
        return self.__reporting.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': str(view_id),
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        'metrics': [{'expression': m} for m in metrics],
                        'dimensions': [{'name': d} for d in dimensions],
                        'pageSize': 100000
                    }]
            }
        ).execute()

    def get_report(self,
                   view_id: int,
                   dimensions: List[str] = ['ga:source', 'ga:medium'],
                   metrics: List[str] = ["ga:sessions"],
                   start_date: str = '30daysAgo',
                   end_date: str = 'yesterday'):
        logging.info(f"Analytics::get_report::{view_id}")
        results = self.__get_report(view_id=view_id,
                                    dimensions=dimensions,
                                    metrics=metrics,
                                    start_date=start_date,
                                    end_date=end_date)
        return Analytics.report_to_df(results)

    def get_all_reports(self,
                        dimensions: List[str] = ['ga:source', 'ga:medium'],
                        metrics: List[str] = ["ga:sessions"],
                        start_date: str = '30daysAgo',
                        end_date: str = 'yesterday'):
        views = self.list_views()
        df_list = []
        for view in views:
            df_view = self.get_report(view_id=view["id"],
                                      dimensions=dimensions,
                                      metrics=metrics,
                                      start_date=start_date,
                                      end_date=end_date)
            df_view["view_id"] = view["id"]
            df_view["view_name"] = view["name"]
            df_view["view_accountId"] = view["accountId"]
            df_view["view_webPropertyId"] = view['webPropertyId']
            df_list.append(df_view)
        return pd.concat(df_list)

    @staticmethod
    def report_to_df(analytics_report):
        report = analytics_report['reports'][0]
        dimensions = [d.replace("ga:", "")
                      for d in report['columnHeader']['dimensions']]
        metrics = [m['name'].replace("ga:", "") for m in report['columnHeader']
                   ['metricHeader']['metricHeaderEntries']]
        headers = [*dimensions, *metrics]

        data_rows = report['data'].get('rows', [])
        data = []
        for row in data_rows:
            data.append([*row['dimensions'], *row['metrics'][0]['values']])
        df = pd.DataFrame(data=data, columns=headers)
        if "date" in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        for metric in metrics:
            df[metric] = pd.to_numeric(df[metric], errors='coerce')

        return df
