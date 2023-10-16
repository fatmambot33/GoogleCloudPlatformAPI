import csv
import datetime
import gzip
import logging
import os
import shutil
import tempfile
from typing import Dict, List, Literal, Optional, TypedDict, Union

import pandas as pd
import pytz
from googleads import ad_manager, errors

from . import APP_NAME, NETWORK_CODE, GAM_VERSION, PYTZ_TIMEZONE
from .ServiceAccount import ServiceAccount
from .Utils import ListHelper


# region objects
gam_adUnit = Dict[int, bool]

gam_adUnits = List[gam_adUnit]
gam_targetingValues = List[int]
logical_operator = Literal["AND", "OR"]
condition_operator = Literal["AND", "OR"]
operator = Literal["AND", "OR"]
status_ = Literal["AND", "OR"]
creativePlaceholders = [
    {
        'size': {
            'width': '300',
            'height': '600'
        }
    },
    {
        'size': {
            'width': '970',
            'height': '250'
        }
    },
    {
        'size': {
            'width': '300',
            'height': '250'
        }
    },
    {
        'size': {
            'width': '320',
            'height': '100'
        }
    }
]


class keyValuePair(TypedDict):
    customTargetingKeyId: int
    id: int
    name: str
    displayName: str
    matchType: str
    status: status_


class customCriteria(TypedDict):
    keyId: int
    valueIds: List[int]
    operator: operator


class customCriteriaSubSet(TypedDict):
    logicalOperator: logical_operator
    children: List[customCriteria]


# customCriteriaNode = TypedDict('customCriteriaNode ', {customCriteriaSubSet})


class customCriteriaSet(TypedDict):
    logicalOperator: logical_operator
    children: List[customCriteriaSubSet]


class targeting(TypedDict):
    geoTargeting: int
    inventoryTargeting: str
    customTargeting: customCriteriaSet


class targetingPreset(TypedDict):
    id: int
    name: str
    targeting: targeting
# endregion


class GamClient(ad_manager.AdManagerClient):
    def __init__(self,
                 app_name: Optional[str] = APP_NAME,
                 network_code:  Optional[str] = NETWORK_CODE):
        oauth2_client = ServiceAccount.get_service_account_client()
        super().__init__(oauth2_client, app_name, network_code=network_code)

    def get_service(self, service_name: str, gam_version: str):
        return self.GetService(service_name=service_name,
                               version=gam_version)

    def get_data_downloader(self, gam_version: str):
        return self.GetDataDownloader(version=gam_version)


class Audience(GamClient):
    __service_name = 'AudienceSegmentService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.__service_name,
                                                    gam_version=gam_version)

    def create(self, name, description, custom_targeting, pageviews: int = 1, recencydays: int = 1, membershipexpirationdays: int = 90, network_code=NETWORK_CODE):
        # Initialize appropriate services.
        network_service = Network(
            app_name=APP_NAME, network_code=network_code)

        if network_service is not None:

            # Get the root ad unit ID used to target the entire network.
            root_ad_unit_id = network_service.effectiveRootAdUnitId()

            # Create inventory targeting (pointed at root ad unit i.e. the whole network)
            inventory_targeting = {
                'targetedAdUnits': [
                    {'adUnitId': root_ad_unit_id}
                ]
            }

            # Create the custom criteria set.
            top_custom_criteria_set = custom_targeting

            # Create the audience segment rule.
            rule = {
                'inventoryRule': inventory_targeting,
                'customCriteriaRule': custom_targeting
            }

            # Create an audience segment.
            audience_segment = [
                {
                    'xsi_type': 'RuleBasedFirstPartyAudienceSegment',
                    'name': name,
                    'description': description,
                    'pageViews': pageviews,
                    'recencyDays': recencydays,
                    'membershipExpirationDays': membershipexpirationdays,
                    'rule': rule
                }
            ]
            audience_segments = self.__gam_service.createAudienceSegments()

            for created_audience_segment in audience_segments:
                logging.info('An audience segment with ID "%s", name "%s", and type "%s" '
                             'was created.' % (created_audience_segment['id'],
                                               created_audience_segment['name'],
                                               created_audience_segment['type']))

    def list(self):
        # Initialize appropriate service.
        audience_segment_service = self.GetService(
            self.__service_name, version=GAM_VERSION)
        # Create a statement to select audience segments.
        statement = (ad_manager.StatementBuilder(version=GAM_VERSION)
                     .Where('Type = :type')
                     .WithBindVariable('type', 'FIRST_PARTY'))
        results = []
        # Retrieve a small amount of audience segments at a time, paging
        # through until all audience segments have been retrieved.
        while True:
            logging.info(
                'getAudienceSegmentsByStatement:statement.offset:'+str(statement.offset))
            response = audience_segment_service.getAudienceSegmentsByStatement(
                statement.ToStatement())
            if 'results' in response and len(response['results']):
                results = results+response['results']
                statement.offset += statement.limit
            else:
                break

        return results

    def list_all(self):
        # Initialize appropriate service.
        audience_segment_service = self.GetService(service_name=self.__service_name,
                                                   version=GAM_VERSION)

        # Create a statement to select audience segments.
        statement = ad_manager.StatementBuilder(version=GAM_VERSION)
        results = []

        # Retrieve a small amount of audience segments at a time, paging
        # through until all audience segments have been retrieved.
        while True:
            response = audience_segment_service.getAudienceSegmentsByStatement(
                statement.ToStatement())
            if 'results' in response and len(response['results']):

                for audience_segment in response['results']:
                    # Print out some information for each audience segment.
                    print('Audience segment with ID "%d", name "%s", and size "%d" was '
                          'found.\n' % (audience_segment['id'], audience_segment['name'],
                                        audience_segment['size']))

                results = results+response['results']
                statement.offset += statement.limit
            else:
                break

        return results

    def update(self, audience_segment_id, name, description, custom_targeting_key_id, custom_targeting_value_id, pageviews=1, recencydays=1, membershipexpirationdays=90):
        # Initialize appropriate service.
        audience_segment_service = self.GetService(service_name=self.__service_name,
                                                   version=GAM_VERSION)

        # Create statement object to get the specified first party audience segment.
        statement = (ad_manager.StatementBuilder(version=GAM_VERSION)
                     .Where('Type = :type AND Id = :audience_segment_id')
                     .WithBindVariable('audience_segment_id',
                                       int(audience_segment_id))
                     .WithBindVariable('type', 'FIRST_PARTY')
                     .Limit(1))

        # Get audience segments by statement.
        response = audience_segment_service.getAudienceSegmentsByStatement(
            statement.ToStatement())

        if 'results' in response and len(response['results']):
            updated_audience_segments = []
            for audience_segment in response['results']:
                print('Audience segment with id "%s" and name "%s" will be updated.'
                      % (audience_segment['id'], audience_segment['name']))

                audience_segment['membershipExpirationDays'] = membershipexpirationdays
                updated_audience_segments.append(audience_segment)

            audience_segments = audience_segment_service.updateAudienceSegments(
                updated_audience_segments)

            for audience_segment in audience_segments:
                print('Audience segment with id "%s" and name "%s" was updated' %
                      (audience_segment['id'], audience_segment['name']))
        else:
            print('No audience segment found to update.')


class Network():
    __service_name: str = 'NetworkService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.__service_name,
                                                    gam_version=gam_version)

    def effectiveRootAdUnitId(self) -> int:
        current_network = self.__gam_service.getCurrentNetwork()
        return int(current_network['effectiveRootAdUnitId'])


class CustomTargeting():
    __service_name = 'CustomTargetingService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.__service_name,
                                                    gam_version=gam_version)

    def get_key_value_pairs(self, targeting_key_id: int) -> List[keyValuePair]:
        logging.info(
            'AdManager::CustomTargeting::get_key_value_pairs::' + str(targeting_key_id))
        # Create a statement to select custom targeting values.
        key_value_pairs_statement = (ad_manager.StatementBuilder(version=GAM_VERSION)
                                     .Where('customTargetingKeyId IN (:id) and status=\'ACTIVE\'')) \
            .WithBindVariable('id', targeting_key_id)

        # Retrieve a small amount of custom targeting values at a time, paging
        # through until all custom targeting values have been retrieved.
        key_value_pairs_list = []
        while True:
            response = self.__gam_service.getCustomTargetingValuesByStatement(
                key_value_pairs_statement.ToStatement())
            if 'results' in response and len(response['results']):
                for custom_targeting_value in response['results']:
                    custom_targeting_value: keyValuePair = custom_targeting_value
                    key_value_pairs_list.append(custom_targeting_value)
                key_value_pairs_statement.offset += key_value_pairs_statement.limit
            else:
                break
        return key_value_pairs_list

    def delete_key_value_pairs(self, targeting_key_id: int, key_value_pairs: List[keyValuePair]):
        logging.info(
            'AdManager::CustomTargeting::delete_key_value_pairs::' + str(targeting_key_id))
        action = {'xsi_type': 'DeleteCustomTargetingValues'}
        key_value_pairs_slices = ListHelper.chunk_list(key_value_pairs, 100)
        for key_value_pairs_slice in key_value_pairs_slices:
            value_statement = (ad_manager.StatementBuilder(version=GAM_VERSION)
                               .Where('customTargetingKeyId = :keyId '
                                      'AND id IN (%s)' % ', '.join([str(key_value_pair["id"]) for key_value_pair in key_value_pairs_slice]))
                               .WithBindVariable('keyId', targeting_key_id))
            logging.info('DeleteCustomTargetingValues:'+', '.join(
                [str(key_value_pair["name"]) for key_value_pair in key_value_pairs_slice]))

            result = self.__gam_service.performCustomTargetingValueAction(
                action, value_statement.ToStatement())
            if result:
                logging.info('numChanges:'+str(result['numChanges']))

    def update_key_value_pairs(self, key_value_pairs: List[keyValuePair]):
        logging.info('AdManager::CustomTargeting::dupdate_key_value_pairs')

        updated_key_value_pairs = self.__gam_service.updateCustomTargetingValues(
            key_value_pairs)

        # Display results.
        for updated_key_value_pair in updated_key_value_pairs:
            logging.info('Custom targeting value with id "%s", name "%s", and display'
                         ' name "%s" was updated.'
                         % (updated_key_value_pair['id'], updated_key_value_pair['name'], updated_key_value_pair['displayName']))

    def create_key_value_pairs(self, created_values: keyValuePair):
        logging.info('AdManager::CustomTargeting::create_key_value_pair')

        values = self.__gam_service.createCustomTargetingValues(
            created_values)

        # Display results.
        for value in values:
            logging.info('Custom targeting value with id "%s", name "%s", and display'
                         ' name "%s" was created.'
                         % (value['id'], value['name'], value['displayName']))


class TargetingPreset():
    __service_name = 'TargetingPresetService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.__service_name,
                                                    gam_version=gam_version)

    def get_targeting_presets_by_prefix(self, targeting_preset_prefix: str):
        logging.info('TargetingPreset::get_targeting_presets_by_prefix:' +
                     targeting_preset_prefix)
        # Create a statement to select targeting presets
        targeting_statement = (ad_manager.StatementBuilder(version=GAM_VERSION)
                               .Where("name LIKE '" + targeting_preset_prefix + "%'"))

        # Retrieve a small amount of custom targeting values at a time, paging
        # through until all custom targeting values have been retrieved.
        targeting_presets = {}
        while True:
            response = self.__gam_service.getTargetingPresetsByStatement(
                targeting_statement.ToStatement())
            if 'results' in response and len(response['results']):
                for targeting_preset in response['results']:
                    targeting_presets[targeting_preset['name']
                                      ] = targeting_preset
                targeting_statement.offset += targeting_statement.limit
            else:
                break
        return targeting_presets


class Report():
    service_name = 'ReportService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.service_name,
                                                    gam_version=gam_version)
        self.data_downloader = gam_client.get_data_downloader(
            gam_version=gam_version)

    def __get_report_by_report_job(self, report_job):

        logging.info('Report::___get_report_by_report_job')
        # Initialize a DataDownloader.
        report_job_id = None
        report_downloader = self.data_downloader

        try:
            # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
        except errors.AdManagerReportError as e:
            print('Failed to generate report. Error was: %s' % e)
        logging.info('report generated')
        export_format = 'CSV_DUMP'

        report_file_gz = tempfile.NamedTemporaryFile(
            suffix='.csv.gz', delete=False)
        report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

        # Download report data.
        report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file_gz)

        report_file_gz.close()

        with gzip.open(report_file_gz.name, 'rb') as f_in:
            with open(report_file.name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(report_file_gz.name)

        with open(report_file.name) as f:
            report_data = [{k: v for k, v in row.items()}
                           for row in csv.DictReader(f, skipinitialspace=True)]
        os.remove(report_file.name)
        return report_data

    @staticmethod
    def gen_report_statement(ad_units: Optional[Union[int, List[int]]] = None,
                             targeting_value_ids: Optional[gam_targetingValues] = None,
                             order_id: Optional[Union[int, List[int]]] = None):
        logging.info('Report::gen_report_statement')
        where_conditions = []
        if ad_units is not None:
            if type(ad_units) == list:
                where_conditions.append(
                    f'PARENT_AD_UNIT_ID IN ({", ".join(str(value) for value in ad_units)})')
            elif type(ad_units) == int:
                where_conditions.append(f'PARENT_AD_UNIT_ID IN ({ad_units}')

        if targeting_value_ids is not None:
            where_conditions.append(
                f'CUSTOM_TARGETING_VALUE_ID  IN ({", ".join(str(value) for value in targeting_value_ids)})')
        if order_id is not None:
            if type(order_id) == int:
                where_conditions.append(
                    f'ORDER_ID IN ({order_id})')
            elif type(order_id) == List[int]:
                where_conditions.append(
                    f'ORDER_ID IN ({",".join(str(value) for value in order_id)})')
        where_statement = " AND ".join(where_conditions)
        built_statement = (ad_manager.StatementBuilder(version=GAM_VERSION)
                           .Where(where_statement)
                           .Limit(0)
                           .Offset(None))

        return built_statement

    @ staticmethod
    def gen_report_query(report_statement,
                         report_end: datetime.date = datetime.date.today(),
                         report_days: int = 7,
                         dimensions: Optional[List[str]] = None,
                         metrics: Optional[List[str]] = None,
                         ad_unit_view: Optional[str] = 'TOP_LEVEL'):
        logging.info('api_build_report_query')
        if dimensions is None:
            dimensions = ['DATE', 'AD_UNIT_NAME', 'CUSTOM_TARGETING_VALUE_ID']
        if metrics is None:
            metrics = ['TOTAL_CODE_SERVED_COUNT',
                       'AD_SERVER_IMPRESSIONS',
                       'AD_SERVER_CLICKS',
                       'ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS',
                       'ADSENSE_LINE_ITEM_LEVEL_CLICKS',
                       'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
                       'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE']
        if ad_unit_view is None:
            ad_unit_view = 'TOP_LEVEL'

        start_date = report_end - datetime.timedelta(days=report_days)

        # Create report job.
        report_query_job = {
            'reportQuery': {
                'dimensions': dimensions,
                'adUnitView': ad_unit_view,
                'statement': report_statement.ToStatement(),
                'columns': metrics,
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': report_end
            }
        }
        return report_query_job

    @ staticmethod
    def normalise_report(data_frame: pd.DataFrame):
        for col in data_frame.columns:
            new_name = col.strip()
            new_name = new_name.lower()
            new_name = new_name.replace(' ', '_')
            new_name = new_name.replace('-', '_')
            if 'column.' in new_name:
                new_name = new_name.replace('column.', '')
                data_frame[col] = pd.to_numeric(data_frame[col])
                data_frame[col].fillna(0)

            if 'dimension.' in new_name:
                new_name = new_name.replace('dimension.', '')

                if new_name in ('date', 'export_date'):
                    pd.to_datetime(data_frame[col])
                else:
                    data_frame[col] = data_frame[col].astype(str)
            data_frame.rename(columns={col: new_name}, inplace=True)
        if 'ad_unit_4' in data_frame.columns and 'ad_unit_5' not in data_frame.columns:
            data_frame['ad_unit_5'] = '-'
            data_frame['ad_unit_id_5'] = '-'
        return data_frame.reindex(sorted(data_frame.columns), axis=1)

    def get_report_dataframe(self,
                             ad_units: Optional[Union[int, List[int]]] = None,
                             targeting_value_ids: Optional[gam_targetingValues] = None,
                             report_date: datetime.date = datetime.date.today(),
                             days: int = 7,
                             dimensions: Optional[List[str]] = None,
                             metrics: Optional[List[str]] = None,
                             ad_unit_view: Optional[str] = 'TOP_LEVEL') -> pd.DataFrame:
        if type(ad_units) == int:
            ad_units = [ad_units]
        statement = self.gen_report_statement(
            ad_units=ad_units, targeting_value_ids=targeting_value_ids)
        df = self.get_report_dataframe_by_statement(statement=statement,
                                                    report_date=report_date,
                                                    days=days,
                                                    dimensions=dimensions,
                                                    metrics=metrics,
                                                    ad_unit_view=ad_unit_view)
        return df

    def get_report_dataframe_by_statement(self,
                                          statement,
                                          report_date: datetime.date = datetime.date.today(),
                                          days: int = 7,
                                          dimensions: Optional[List[str]] = None,
                                          metrics: Optional[List[str]] = None,
                                          ad_unit_view: Optional[str] = 'TOP_LEVEL') -> pd.DataFrame:
        report_job = self.gen_report_query(statement,
                                           report_date,
                                           days,
                                           dimensions,
                                           metrics,
                                           ad_unit_view)
        report_content = self.__get_report_by_report_job(report_job)
        df = pd.DataFrame.from_dict(
            report_content,  # type: ignore
            orient='columns')
        return Report.normalise_report(df)


class Forecast():
    service_name = 'ForecastService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.service_name,
                                                    gam_version=gam_version)

    class forecastItem(TypedDict):
        date: datetime.date
        matched: int
        available: int
        possible: int
        name: str

    @staticmethod
    def __gen_line_item(targetedAdUnits,
                        creativePlaceholders,
                        report_date: datetime.datetime = datetime.datetime.now(
                            tz=pytz.timezone(PYTZ_TIMEZONE)) + datetime.timedelta(days=1),
                        days: int = 30):
        logging.info('Report::__gen_line_item')
        prospective_line_item = {
            'lineItem': {
                'targeting': {
                    'inventoryTargeting': {
                        'targetedAdUnits': targetedAdUnits
                    }
                },
                'creativePlaceholders': creativePlaceholders,
                'lineItemType': 'STANDARD',
                'startDateTimeType': 'IMMEDIATELY',
                'endDateTime': report_date+datetime.timedelta(days),
                'costType': 'CPM',
                'costPerUnit': {
                    'currencyCode': 'USD',
                    'microAmount': '2000000'
                },
                'primaryGoal': {
                    'units': '50',
                    'unitType': 'IMPRESSIONS',
                    'goalType': 'LIFETIME'
                },

                'creativeRotationType': 'EVEN',
                'discountType': 'PERCENTAGE',
            },
            'advertiserId': None
        }
        return prospective_line_item

    @staticmethod
    def __gen_forecast_options(targets_list, report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)), days: int = 30):
        logging.info('Forecast::__gen_forecast_options')
        targets = []
        for target in targets_list:
            targets.append({'name': target.get('name'),
                            'targeting': {
                'customTargeting': {
                    'xsi_type': 'CustomCriteriaSet',
                    'logicalOperator': 'OR',
                    'children':
                    {
                        'xsi_type': 'CustomCriteria',
                        'keyId': target.get('keyId'),
                        'valueIds': [target.get('valueIds')],
                        'operator': 'IS'
                    }

                }
            }})
        timeWindows = []
        for d in range(days):
            timeWindows.append(report_date+datetime.timedelta(days=d))
        forecast_options = {
            'includeContendingLineItems': True,
            # The field includeTargetingCriteriaBreakdown can only be set if
            # breakdowns are not manually specified.
            # 'includeTargetingCriteriaBreakdown': True,
            'breakdown': {
                'timeWindows': timeWindows,
                'targets': targets
            }
        }
        return forecast_options

    @staticmethod
    def __gen_forecast_options_by_targeting_presets(targeting_presets,
                                                    report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)),  days: int = 30):
        logging.info('Forecast::__gen_forecast_options_by_targeting_presets')
        timeWindows = []
        for d in range(days):
            timeWindows.append(report_date+datetime.timedelta(days=d))

        targets = []
        for targeting_preset in targeting_presets:
            targets.append({"name": targeting_preset.name, "targeting": {
                "customTargeting": targeting_preset.targeting.customTargeting
            }})
        forecast_options = {
            'includeContendingLineItems': True,
            # The field includeTargetingCriteriaBreakdown can only be set if
            # breakdowns are not manually specified.
            # 'includeTargetingCriteriaBreakdown': True,
            'breakdown': {
                'timeWindows': timeWindows,
                'targets': targets
            }
        }
        return forecast_options

    def get_forecast(self,
                     targetedAdUnits,
                     creativePlaceholders,
                     targets_list,
                     report_date: datetime.datetime = datetime.datetime.now(
                         tz=pytz.timezone(PYTZ_TIMEZONE)),
                     days: int = 30) -> List[forecastItem]:
        logging.info('Forecast::get_forecast')
        # Create prospective line item.
        prospective_line_item = self.__gen_line_item(
            targetedAdUnits, creativePlaceholders, report_date, days)

        forecast_options = self.__gen_forecast_options(
            targets_list, report_date, days)

        # Get forecast.
        forecast = self.__gam_service.getAvailabilityForecast(
            prospective_line_item, forecast_options)

        forecast_list = []
        if 'breakdowns' in forecast and len(forecast['breakdowns']):
            for breakdown in forecast['breakdowns']:
                for breakdown_entry in breakdown['breakdownEntries']:
                    value_date = breakdown['startTime']['date']
                    dt = datetime.date(
                        value_date['year'], value_date['month'], value_date['day'])
                    item = Forecast.forecastItem(date=dt,
                                                 matched=breakdown_entry['forecast']['matched'],
                                                 available=breakdown_entry['forecast']['available'],
                                                 possible=breakdown_entry['forecast'][
                                                     'possible'] if 'possible' in breakdown_entry['forecast'] else 0,
                                                 name=breakdown_entry['name'] if 'name' in breakdown_entry else "")
                    forecast_list.append(item)

        return forecast_list

    def get_forecast_by_targeting_preset(self,
                                         targetedAdUnits,
                                         creativePlaceholders,
                                         targeting_presets,
                                         report_date: datetime.datetime = datetime.datetime.now(
                                             tz=pytz.timezone(PYTZ_TIMEZONE)),
                                         days: int = 30) -> List[forecastItem]:
        logging.info('Forecast::get_forecast_by_targeting_preset')
        # Create prospective line item.
        prospective_line_item = self.__gen_line_item(
            targetedAdUnits, creativePlaceholders, report_date, days)

        forecast_options = self.__gen_forecast_options_by_targeting_presets(
            targeting_presets, report_date, days)

        # Get forecast.
        forecast = self.__gam_service.getAvailabilityForecast(
            prospective_line_item, forecast_options)

        forecast_list = []
        if 'breakdowns' in forecast and len(forecast['breakdowns']):
            for breakdown in forecast['breakdowns']:

                for breakdown_entry in breakdown['breakdownEntries']:
                    value_date = breakdown['startTime']['date']
                    dt = datetime.date(
                        value_date['year'], value_date['month'], value_date['day'])
                    item = Forecast.forecastItem(date=dt,
                                                 matched=breakdown_entry['forecast']['matched'],
                                                 available=breakdown_entry['forecast']['available'],
                                                 possible=breakdown_entry['forecast'][
                                                     'possible'] if 'possible' in breakdown_entry['forecast'] else 0,
                                                 name=breakdown_entry['name'] if 'name' in breakdown_entry else "")
                    forecast_list.append(item)
        return forecast_list


class Traffic():
    service_name = 'ForecastService'

    def __init__(self,
                 app_name: str = APP_NAME,
                 network_code:  str = NETWORK_CODE,
                 gam_version: str = GAM_VERSION):
        gam_client = GamClient(app_name=app_name,
                               network_code=network_code)
        self.__gam_service = gam_client.get_service(service_name=self.service_name,
                                                    gam_version=gam_version)

    class trafficItem(TypedDict):
        date: datetime.date
        impressions: int

    def get_traffic(self,
                    inventory_targeting=None,
                    custom_targeting=None,
                    report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)),
            days: int = 30) -> List[trafficItem]:
        """
        :rtype: [{str, int}]
        :param client: the ad manager client
        :param custom_targeting:
        :param inventory_targeting:
        :param days: int the number of days to forecast
        :return: the forecasted impressions per day
        """
        logging.info('Traffic::get_traffic')

        def time_series_to_list(time_series):
            logging.info('Traffic::time_series_to_list')
            date_range = time_series['timeSeriesDateRange']
            time_series_start_date = datetime.date(
                date_range['startDate']['year'],
                date_range['startDate']['month'],
                date_range['startDate']['day']
            )
            time_series_end_date = datetime.date(
                date_range['endDate']['year'],
                date_range['endDate']['month'],
                date_range['endDate']['day']
            )
            time_series_forecast_data = []
            offset = 0
            current_date = time_series_start_date
            while current_date <= time_series_end_date:
                time_series_forecast_data.append(
                    {'date': current_date.isoformat(), 'impressions': time_series['values'][offset]})
                offset += 1
                current_date = time_series_start_date + \
                    datetime.timedelta(days=offset)
            return time_series_forecast_data

        # the time-lapse to for forecast
        start_date = report_date.date() - datetime.timedelta(days=days)
        end_date = report_date.date() + datetime.timedelta(days=days)

        if inventory_targeting is None:
            inventory_targeting = Network().effectiveRootAdUnitId()

        # Create targeting.
        targeting = {
            'inventoryTargeting': inventory_targeting,
            'customTargeting': custom_targeting
        }
        from time import sleep

        # Request the traffic forecast data.
        start = datetime.datetime.now()
        traffic_data = self.__gam_service.getTrafficData({
            'targeting': targeting,
            'requestedDateRange': {
                'startDate': start_date,
                'endDate': end_date
            }
        })
        wait_time = 2-(datetime.datetime.now()-start).total_seconds()
        if wait_time > 0:
            sleep(wait_time)
        return time_series_to_list(traffic_data['historicalTimeSeries']) + time_series_to_list(traffic_data['forecastedTimeSeries'])

    def get_traffic_by_targeting_preset(self,
                                        inventory_targeting,
                                        targeting_preset,
                                        report_date: datetime.datetime = datetime.datetime.now(
                                            tz=pytz.timezone(PYTZ_TIMEZONE)),
                                        days: int = 1) -> List[trafficItem]:
        """
        :rtype: [{str, int}]
        :param client: the ad manager client
        :param custom_targeting:
        :param inventory_targeting:
        :param days: int the number of days to forecast
        :return: the forecasted impressions per day
        """
        logging.info('Traffic:get_traffic_by_targeting_preset')
        return self.get_traffic(inventory_targeting=inventory_targeting,
                                custom_targeting=targeting_preset.targeting.customTargeting,
                                report_date=report_date,
                                days=days)
