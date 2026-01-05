"""
Helpers for interacting with the Google Ad Manager API.

This module provides a set of classes to simplify interactions with various
services within the Google Ad Manager API. It handles client authentication,
service discovery, and provides methods for common operations like reporting,
forecasting, and managing audiences.

Public Classes
--------------
- GamClient: A client for the Google Ad Manager API.
- AudienceService: A wrapper for the Audience service.
- NetworkService: A wrapper for the Network service.
- CustomTargetingService: A wrapper for the CustomTargeting service.
- TargetingPresetService: A wrapper for the TargetingPreset service.
- ReportService: A wrapper for the Report service.
- ForecastService: A wrapper for the Forecast service.
- TrafficService: A wrapper for the Traffic service.
"""

import csv
import datetime
import gzip
import logging
import os
import shutil
import tempfile
from enum import Enum
from time import sleep
from typing import Any, Dict, List, Literal, Optional, TypedDict, Union

import pandas as pd
import pytz
from googleads import ad_manager, errors

from .Oauth import ServiceAccount
from .Utils import ListHelper


PYTZ_TIMEZONE = "UTC"
AD_UNIT_VIEW = "TOP_LEVEL"
METRICS = [
    "TOTAL_CODE_SERVED_COUNT",
    "AD_SERVER_IMPRESSIONS",
    "AD_SERVER_CLICKS",
    "ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS",
    "ADSENSE_LINE_ITEM_LEVEL_CLICKS",
    "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
    "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE",
]

DIMENSIONS = ["DATE", "AD_UNIT_NAME", "CUSTOM_TARGETING_VALUE_ID"]

GAM_VERSION = "v202505"
NETWORK_CODE = "5574"
APP_NAME = "AdManagerAPIClient"

# region objects
Operator = Literal["AND", "OR"]


class AdUnitView(Enum):
    """Enum for the Ad Unit view in a report."""

    TOP_LEVEL = "TOP_LEVEL"
    FLAT = "FLAT"


class KeyValuePair(TypedDict):
    """A TypedDict for a custom targeting key-value pair."""

    customTargetingKeyId: int
    id: int
    name: str
    displayName: str
    matchType: str
    status: Operator


class CustomCriteria(TypedDict):
    """A TypedDict for a custom criteria."""

    keyId: int
    valueIds: List[int]
    operator: Operator


class CustomCriteriaSubSet(TypedDict):
    """A TypedDict for a subset of custom criteria."""

    logicalOperator: Operator
    children: List[CustomCriteria]


class CustomCriteriaSet(TypedDict):
    """A TypedDict for a set of custom criteria."""

    logicalOperator: Operator
    children: List[CustomCriteriaSubSet]


class Targeting(TypedDict):
    """A TypedDict for targeting information."""

    customTargeting: CustomCriteriaSet


class TargetingPreset(TypedDict):
    """A TypedDict for a targeting preset."""

    id: int
    name: str
    targeting: Targeting


class Size(TypedDict):
    """A TypedDict for a size."""

    width: str
    height: str


class CreativePlaceholder(TypedDict):
    """A TypedDict for a creative placeholder."""

    size: Size


class ForecastItem(TypedDict):
    """A TypedDict for a forecast item."""

    date: datetime.date
    matched: int
    available: int
    possible: int
    name: str


class trafficItem(TypedDict):
    """A TypedDict for a traffic item."""

    date: datetime.date
    impressions: int


# endregion


class GamClient(ad_manager.AdManagerClient):
    """
    A client for the Google Ad Manager API.

    This class extends the base ``ad_manager.AdManagerClient`` to simplify
    initialization with service account credentials.

    Attributes
    ----------
    app_name : str
        The name of the application.
    network_code : str
        The Ad Manager network code.

    Methods
    -------
    get_service(service_name, gam_version)
        Get a service client for the Ad Manager API.
    get_data_downloader(gam_version)
        Get a data downloader for the Ad Manager API.
    """

    def __init__(self, app_name: str = APP_NAME, network_code: str = NETWORK_CODE):
        """
        Initialise the GamClient.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        """
        logging.debug(f"GamClient::__init__::{network_code}")
        oauth2_client = ServiceAccount.get_service_account_client()
        super().__init__(oauth2_client, app_name, network_code=network_code)

    def get_service(self, service_name: str, gam_version: str) -> Any:
        """
        Get a service client for the Ad Manager API.

        Parameters
        ----------
        service_name : str
            The name of the service to get.
        gam_version : str
            The version of the API to use.

        Returns
        -------
        Any
            An instance of the requested service client.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.AdManager import GamClient

        # Assumes GOOGLE_APPLICATION_CREDENTIALS is set
        gam_client = GamClient()
        network_service = gam_client.get_service(
            service_name="NetworkService",
            gam_version="v202505"
        )
        ```
        """
        logging.debug(f"GamClient::get_service::{service_name}::{gam_version}")
        return self.GetService(service_name=service_name, version=gam_version)

    def get_data_downloader(self, gam_version: str) -> ad_manager.DataDownloader:
        """
        Get a data downloader for the Ad Manager API.

        Parameters
        ----------
        gam_version : str
            The version of the API to use.

        Returns
        -------
        googleads.ad_manager.DataDownloader
            A data downloader instance.

        Examples
        --------
        ```python
        from GoogleCloudPlatformAPI.AdManager import GamClient

        # Assumes GOOGLE_APPLICATION_CREDENTIALS is set
        gam_client = GamClient()
        data_downloader = gam_client.get_data_downloader(gam_version="v202505")
        ```
        """
        logging.debug(f"GamClient::get_data_downloader:{gam_version}")
        return self.GetDataDownloader(version=gam_version)


class AudienceService:
    """A wrapper for the Audience service of the Ad Manager API.

    Methods
    -------
    create(...)
        Create a new audience segment.
    list()
        List all first-party audience segments.
    list_all()
        List all audience segments of any type.
    update(...)
        Update an existing audience segment.
    """

    _service_name = "AudienceService"
    _gam_service: Any

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the AudienceService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        logging.debug(
            f"Audience::__init__:{self._service_name}::{network_code}::{gam_version}"
        )
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self._gam_service = gam_client.get_service(
            service_name=self._service_name, gam_version=gam_version
        )

    def create(
        self,
        name: str,
        description: str,
        custom_targeting: CustomCriteriaSet,
        pageviews: int = 1,
        recencydays: int = 1,
        membershipexpirationdays: int = 90,
        network_code: str = NETWORK_CODE,
    ) -> None:
        """
        Create a new audience segment.

        Parameters
        ----------
        name : str
            The name of the audience segment.
        description : str
            The description of the audience segment.
        custom_targeting : CustomCriteriaSet
            The custom targeting rules for the audience.
        pageviews : int, optional
            The number of pageviews required. Defaults to 1.
        recencydays : int, optional
            The number of recency days. Defaults to 1.
        membershipexpirationdays : int, optional
            The membership expiration in days. Defaults to 90.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug(f"Audience::create:{name}")
        # Initialize appropriate services.
        network_service = NetworkService(app_name=APP_NAME, network_code=network_code)
        # Get the root ad unit ID used to target the entire network.
        root_ad_unit_id = network_service.effective_root_ad_unit_id()

        # Create inventory targeting (pointed at root ad unit i.e. the whole network)
        inventory_targeting = {"targetedAdUnits": [{"adUnitId": root_ad_unit_id}]}
        # Create the audience segment rule.
        rule = {
            "inventoryRule": inventory_targeting,
            "customCriteriaRule": custom_targeting,
        }
        # Create an audience segment.
        audience_segment = [
            {
                "xsi_type": "RuleBasedFirstPartyAudienceSegment",
                "name": name,
                "description": description,
                "pageViews": pageviews,
                "recencyDays": recencydays,
                "membershipExpirationDays": membershipexpirationdays,
                "rule": rule,
            }
        ]
        audience_segments = self._gam_service.createAudienceSegments(audience_segment)

        for created_audience_segment in audience_segments:
            logging.debug(
                f'An audience segment with ID "{created_audience_segment["id"]}", '
                f'name "{created_audience_segment["name"]}", and type '
                f'"{created_audience_segment["type"]}" was created.'
            )

    def list(self) -> List[Dict[str, Any]]:
        """
        List all first-party audience segments.

        Returns
        -------
        list[dict[str, Any]]
            A list of audience segment objects.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("Audience::list")
        # Create a statement to select audience segments.
        statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("Type = :type")
            .WithBindVariable("type", "FIRST_PARTY")
        )
        results = []
        # Retrieve a small amount of audience segments at a time, paging
        # through until all audience segments have been retrieved.
        while True:
            logging.debug(
                f"getAudienceSegmentsByStatement:statement.offset:{statement.offset}"
            )
            response = self._gam_service.getAudienceSegmentsByStatement(
                statement.ToStatement()
            )
            if "results" in response and len(response["results"]):
                results.extend(response["results"])
                statement.offset += statement.limit
            else:
                break

        return results

    def list_all(self) -> List[Dict[str, Any]]:
        """
        List all audience segments of any type.

        Returns
        -------
        list[dict[str, Any]]
            A list of audience segment objects.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        # Create a statement to select audience segments.
        statement = ad_manager.StatementBuilder(version=GAM_VERSION)
        results = []

        # Retrieve a small amount of audience segments at a time, paging
        # through until all audience segments have been retrieved.
        while True:
            response = self._gam_service.getAudienceSegmentsByStatement(
                statement.ToStatement()
            )
            if "results" in response and len(response["results"]):
                for audience_segment in response["results"]:
                    logging.debug(
                        f'Audience segment with ID "{audience_segment["id"]}", '
                        f'name "{audience_segment["name"]}", and size '
                        f'"{audience_segment["size"]}" was found.'
                    )

                results.extend(response["results"])
                statement.offset += statement.limit
            else:
                break

        return results

    def update(
        self,
        audience_segment_id: int,
        name: str,
        description: str,
        custom_targeting_key_id: int,
        custom_targeting_value_id: int,
        pageviews: int = 1,
        recencydays: int = 1,
        membershipexpirationdays: int = 90,
    ) -> None:
        """
        Update an existing audience segment.

        Parameters
        ----------
        audience_segment_id : int
            The ID of the audience segment to update.
        name : str
            The new name for the audience segment.
        description : str
            The new description for the audience segment.
        custom_targeting_key_id : int
            The ID of the custom targeting key.
        custom_targeting_value_id : int
            The ID of the custom targeting value.
        pageviews : int, optional
            The number of pageviews required. Defaults to 1.
        recencydays : int, optional
            The number of recency days. Defaults to 1.
        membershipexpirationdays : int, optional
            The membership expiration in days. Defaults to 90.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        # Create statement object to get the specified first party audience segment.
        statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("Type = :type AND Id = :audience_segment_id")
            .WithBindVariable("audience_segment_id", int(audience_segment_id))
            .WithBindVariable("type", "FIRST_PARTY")
            .Limit(1)
        )

        # Get audience segments by statement.
        response = self._gam_service.getAudienceSegmentsByStatement(
            statement.ToStatement()
        )

        if "results" in response and len(response["results"]):
            updated_audience_segments = []
            for audience_segment in response["results"]:
                audience_segment["name"] = name
                audience_segment["description"] = description
                audience_segment["membershipExpirationDays"] = membershipexpirationdays
                updated_audience_segments.append(audience_segment)

            audience_segments = self._gam_service.updateAudienceSegments(
                updated_audience_segments
            )

            for audience_segment in audience_segments:
                logging.debug(
                    f'Audience segment with id "{audience_segment["id"]}" and '
                    f'name "{audience_segment["name"]}" was updated'
                )
        else:
            logging.debug("No audience segment found to update.")


class NetworkService:
    """A wrapper for the Network service of the Ad Manager API.

    Methods
    -------
    effective_root_ad_unit_id()
        Get the effective root ad unit ID for the network.
    """

    _service_name = "NetworkService"
    _gam_service: Any

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the NetworkService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self._gam_service = gam_client.get_service(
            service_name=self._service_name, gam_version=gam_version
        )

    def effective_root_ad_unit_id(self) -> int:
        """
        Get the effective root ad unit ID for the network.

        Returns
        -------
        int
            The root ad unit ID.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        current_network = self._gam_service.getCurrentNetwork()
        return int(current_network["effectiveRootAdUnitId"])


class CustomTargetingService:
    """A wrapper for the CustomTargeting service of the Ad Manager API.

    Methods
    -------
    list(targeting_key_id)
        List all active key-value pairs for a given targeting key.
    delete(targeting_key_id, key_value_pairs)
        Delete a list of key-value pairs.
    update(key_value_pairs)
        Update a list of key-value pairs.
    create(created_values)
        Create a list of key-value pairs.
    """

    _service_name = "CustomTargetingService"
    _gam_service: Any

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the CustomTargetingService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self._gam_service = gam_client.get_service(
            service_name=self._service_name, gam_version=gam_version
        )

    def list(self, targeting_key_id: int) -> List[KeyValuePair]:
        """
        List all active key-value pairs for a given targeting key.

        Parameters
        ----------
        targeting_key_id : int
            The ID of the custom targeting key.

        Returns
        -------
        list[KeyValuePair]
            A list of key-value pair objects.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug(
            f"AdManager::CustomTargeting::get_key_value_pairs::{targeting_key_id}"
        )
        # Create a statement to select custom targeting values.
        key_value_pairs_statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("customTargetingKeyId IN (:id) and status='ACTIVE'")
            .WithBindVariable("id", targeting_key_id)
        )

        # Retrieve a small amount of custom targeting values at a time, paging
        # through until all custom targeting values have been retrieved.
        key_value_pairs_list = []
        while True:
            response = self._gam_service.getCustomTargetingValuesByStatement(
                key_value_pairs_statement.ToStatement()
            )
            if "results" in response and len(response["results"]):
                for custom_targeting_value in response["results"]:
                    custom_targeting_value: KeyValuePair = custom_targeting_value
                    key_value_pairs_list.append(custom_targeting_value)
                key_value_pairs_statement.offset += key_value_pairs_statement.limit
            else:
                break
        return key_value_pairs_list

    def delete(
        self, targeting_key_id: int, key_value_pairs: List[KeyValuePair]
    ) -> None:
        """
        Delete a list of key-value pairs.

        Parameters
        ----------
        targeting_key_id : int
            The ID of the custom targeting key.
        key_value_pairs : list[KeyValuePair]
            The list of key-value pairs to delete.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug(
            f"AdManager::CustomTargeting::delete_key_value_pairs::{targeting_key_id}"
        )
        action = {"xsi_type": "DeleteCustomTargetingValues"}
        key_value_pairs_slices = ListHelper.chunk_list(key_value_pairs, 100)
        for key_value_pairs_slice in key_value_pairs_slices:
            value_statement = (
                ad_manager.StatementBuilder(version=GAM_VERSION)
                .Where(
                    "customTargetingKeyId = :keyId "
                    "AND id IN (%s)"
                    % ", ".join(
                        [
                            str(key_value_pair["id"])
                            for key_value_pair in key_value_pairs_slice
                        ]
                    )
                )
                .WithBindVariable("keyId", targeting_key_id)
            )
            logging.debug(
                "DeleteCustomTargetingValues: "
                + f'{", ".join([str(kvp["name"]) for kvp in key_value_pairs_slice])}'
            )

            result = self._gam_service.performCustomTargetingValueAction(
                action, value_statement.ToStatement()
            )
            if result:
                logging.debug(f'numChanges: {result["numChanges"]}')

    def update(self, key_value_pairs: List[KeyValuePair]) -> None:
        """
        Update a list of key-value pairs.

        Parameters
        ----------
        key_value_pairs : list[KeyValuePair]
            The list of key-value pairs to update.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("AdManager::CustomTargeting::dupdate_key_value_pairs")

        updated_key_value_pairs = self._gam_service.updateCustomTargetingValues(
            key_value_pairs
        )

        # Display results.
        for updated_key_value_pair in updated_key_value_pairs:
            logging.debug(
                f'Custom targeting value with id "{updated_key_value_pair["id"]}", '
                f'name "{updated_key_value_pair["name"]}", and display name '
                f'"{updated_key_value_pair["displayName"]}" was updated.'
            )

    def create(self, created_values: List[KeyValuePair]) -> None:
        """
        Create a list of key-value pairs.

        Parameters
        ----------
        created_values : list[KeyValuePair]
            The list of key-value pairs to create.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("AdManager::CustomTargeting::create_key_value_pair")

        values = self._gam_service.createCustomTargetingValues(created_values)

        # Display results.
        for value in values:
            logging.debug(
                f'Custom targeting value with id "{value["id"]}", '
                f'name "{value["name"]}", and display name '
                f'"{value["displayName"]}" was created.'
            )


class TargetingPresetService:
    """A wrapper for the TargetingPreset service of the Ad Manager API.

    Methods
    -------
    create(targeting)
        Create a list of targeting presets.
    update(targeting)
        Update a list of targeting presets.
    list_by_prefix(targeting_preset_prefix)
        List targeting presets that match a given prefix.
    """

    _service_name = "TargetingPresetService"
    _gam_service: Any

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the TargetingPresetService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self._gam_service = gam_client.get_service(
            service_name=self._service_name, gam_version=gam_version
        )

    def create(self, targeting: List[TargetingPreset]) -> None:
        """
        Create a list of targeting presets.

        Parameters
        ----------
        targeting : list[TargetingPreset]
            The list of targeting presets to create.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        self._gam_service.createTargetingPresets(targeting)

    def update(self, targeting: List[TargetingPreset]) -> None:
        """
        Update a list of targeting presets.

        Parameters
        ----------
        targeting : list[TargetingPreset]
            The list of targeting presets to update.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        self._gam_service.updateTargetingPresets(targeting)

    def list_by_prefix(
        self, targeting_preset_prefix: str
    ) -> Dict[str, TargetingPreset]:
        """
        List targeting presets that match a given prefix.

        Parameters
        ----------
        targeting_preset_prefix : str
            The prefix to match against targeting preset names.

        Returns
        -------
        dict[str, TargetingPreset]
            A dictionary of targeting presets, keyed by name.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug(
            f"TargetingPreset::get_targeting_presets_by_prefix:{targeting_preset_prefix}"
        )
        # Create a statement to select targeting presets
        targeting_statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("name LIKE :prefix")
            .WithBindVariable("prefix", f"{targeting_preset_prefix}%")
        )

        # Retrieve a small amount of custom targeting values at a time, paging
        # through until all custom targeting values have been retrieved.
        targeting_presets: Dict[str, TargetingPreset] = {}
        while True:
            response = self._gam_service.getTargetingPresetsByStatement(
                targeting_statement.ToStatement()
            )
            if "results" in response and len(response["results"]):
                for targeting_preset in response["results"]:
                    targeting_presets[targeting_preset["name"]] = targeting_preset
                targeting_statement.offset += targeting_statement.limit
            else:
                break
        return targeting_presets


class ReportService:
    """A wrapper for the Report service of the Ad Manager API.

    Methods
    -------
    gen_report_statement(ad_units=None, targeting_value_ids=None, order_id=None)
        Generate a statement for a report query.
    gen_report_query(...)
        Generate a report query job.
    normalise_report(data_frame)
        Normalise the column names of a report DataFrame.
    get_report_dataframe(...)
        Get a report as a DataFrame.
    get_report_dataframe_by_statement(...)
        Get a report as a DataFrame, using a statement.
    """

    data_downloader: ad_manager.DataDownloader

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the ReportService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self.data_downloader = gam_client.get_data_downloader(gam_version=gam_version)

    def _get_report_by_report_job(
        self, report_job: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Run a report job and return the results.

        Parameters
        ----------
        report_job : dict[str, Any]
            The report job to run.

        Returns
        -------
        list[dict[str, Any]]
            The report data as a list of dictionaries.

        Raises
        ------
        googleads.errors.AdManagerReportError
            If the report fails to generate.
        """
        logging.debug("Report::_get_report_by_report_job")
        # Initialize a DataDownloader.
        report_job_id = None
        report_downloader = self.data_downloader

        try:
            # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
        except errors.AdManagerReportError as e:
            logging.error(f"Failed to generate report. Error was: {e}")
            raise e
        logging.debug("report generated")
        export_format = "CSV_DUMP"

        report_file_gz = tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False)
        report_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)

        # Download report data.
        report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file_gz
        )

        report_file_gz.close()

        with gzip.open(report_file_gz.name, "rb") as f_in:
            with open(report_file.name, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(report_file_gz.name)

        with open(report_file.name) as f:
            report_data = [
                dict(row) for row in csv.DictReader(f, skipinitialspace=True)
            ]
        os.remove(report_file.name)
        return report_data

    @staticmethod
    def gen_report_statement(
        ad_units: Optional[Union[int, List[int]]] = None,
        targeting_value_ids: Optional[List[int]] = None,
        order_id: Optional[Union[int, List[int]]] = None,
    ) -> ad_manager.StatementBuilder:
        """
        Generate a statement for a report query.

        Parameters
        ----------
        ad_units : int or list[int], optional
            The ad unit IDs to filter by.
        targeting_value_ids : list[int], optional
            The custom targeting value IDs to filter by.
        order_id : int or list[int], optional
            The order IDs to filter by.

        Returns
        -------
        googleads.ad_manager.StatementBuilder
            The generated statement builder.
        """
        logging.debug("Report::gen_report_statement")
        where_conditions = []
        if ad_units is not None:
            if isinstance(ad_units, int):
                ad_units = [ad_units]
            where_conditions.append(
                f'PARENT_AD_UNIT_ID IN ({", ".join(str(value) for value in ad_units)})'
            )

        if targeting_value_ids is not None:
            where_conditions.append(
                f'CUSTOM_TARGETING_VALUE_ID  IN ({", ".join(str(value) for value in targeting_value_ids)})'
            )

        if order_id is not None:
            if isinstance(order_id, int):
                order_id = [order_id]
            where_conditions.append(
                f'ORDER_ID IN ({",".join(str(value) for value in order_id)})'
            )
        where_statement = " AND ".join(where_conditions)
        built_statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where(where_statement)
            .Limit(0)
            .Offset(None)
        )

        return built_statement

    @staticmethod
    def gen_report_query(
        report_statement: ad_manager.StatementBuilder,
        report_end: datetime.date = datetime.date.today(),
        report_days: int = 1,
        dimensions: List[str] = DIMENSIONS,
        metrics: List[str] = METRICS,
        ad_unit_view: AdUnitView = AdUnitView.TOP_LEVEL,
    ) -> Dict[str, Any]:
        """
        Generate a report query job.

        Parameters
        ----------
        report_statement : googleads.ad_manager.StatementBuilder
            The statement for the report query.
        report_end : datetime.date, optional
            The end date for the report. Defaults to today.
        report_days : int, optional
            The number of days to include in the report. Defaults to 1.
        dimensions : list[str], optional
            The dimensions to include in the report. Defaults to ``DIMENSIONS``.
        metrics : list[str], optional
            The metrics to include in the report. Defaults to ``METRICS``.
        ad_unit_view : AdUnitView, optional
            The ad unit view for the report. Defaults to ``AdUnitView.TOP_LEVEL``.

        Returns
        -------
        dict[str, Any]
            The report query job.
        """
        logging.debug("api_build_report_query")

        start_date = report_end - datetime.timedelta(days=report_days)

        # Create report job.
        report_query_job = {
            "reportQuery": {
                "dimensions": dimensions,
                "adUnitView": ad_unit_view.name,
                "statement": report_statement.ToStatement(),
                "columns": metrics,
                "dateRangeType": "CUSTOM_DATE",
                "startDate": start_date,
                "endDate": report_end,
            }
        }
        return report_query_job

    @staticmethod
    def normalise_report(data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Normalise the column names of a report DataFrame.

        This method cleans up column names by stripping whitespace, converting
        to lowercase, replacing spaces and hyphens with underscores, and
        removing "column." and "dimension." prefixes. It also handles type
        conversion for numeric and date columns.

        Parameters
        ----------
        data_frame : pandas.DataFrame
            The report DataFrame to normalise.

        Returns
        -------
        pandas.DataFrame
            The normalised DataFrame.
        """
        df = data_frame.copy()
        for col in df.columns:
            new_name = col.strip()
            new_name = new_name.lower()
            new_name = new_name.replace(" ", "_")
            new_name = new_name.replace("-", "_")
            if "column." in new_name:
                new_name = new_name.replace("column.", "")
                df[col] = pd.to_numeric(df[col])
                df[col].fillna(0)

            if "dimension." in new_name:
                new_name = new_name.replace("dimension.", "")

                if new_name in ("date", "export_date"):
                    pd.to_datetime(df[col])
                else:
                    df[col] = df[col].astype(str)
            df = df.rename(columns={col: new_name})
        if "ad_unit_4" in df.columns and "ad_unit_5" not in df.columns:
            df["ad_unit_5"] = "-"
            df["ad_unit_id_5"] = "-"
        return df.reindex(sorted(df.columns), axis=1)

    def get_report_dataframe(
        self,
        ad_units: Optional[Union[int, List[int]]] = None,
        targeting_value_ids: Optional[List[int]] = None,
        report_date: datetime.date = datetime.date.today(),
        days: int = 1,
        dimensions: List[str] = DIMENSIONS,
        metrics: List[str] = METRICS,
        ad_unit_view: str = AD_UNIT_VIEW,
    ) -> pd.DataFrame:
        """
        Get a report as a DataFrame.

        Parameters
        ----------
        ad_units : int or list[int], optional
            The ad unit IDs to filter by.
        targeting_value_ids : list[int], optional
            The custom targeting value IDs to filter by.
        report_date : datetime.date, optional
            The end date for the report. Defaults to today.
        days : int, optional
            The number of days to include in the report. Defaults to 1.
        dimensions : list[str], optional
            The dimensions to include in the report. Defaults to ``DIMENSIONS``.
        metrics : list[str], optional
            The metrics to include in the report. Defaults to ``METRICS``.
        ad_unit_view : str, optional
            The ad unit view for the report. Defaults to ``AD_UNIT_VIEW``.

        Returns
        -------
        pandas.DataFrame
            The report data.

        Raises
        ------
        googleads.errors.AdManagerReportError
            If the report fails to generate.
        """
        if isinstance(ad_units, int):
            ad_units = [ad_units]
        statement = self.gen_report_statement(
            ad_units=ad_units, targeting_value_ids=targeting_value_ids
        )
        df = self.get_report_dataframe_by_statement(
            statement=statement,
            report_date=report_date,
            days=days,
            dimensions=dimensions,
            metrics=metrics,
            ad_unit_view=ad_unit_view,
        )
        return df

    def get_report_dataframe_by_statement(
        self,
        statement: ad_manager.StatementBuilder,
        report_date: datetime.date = datetime.date.today(),
        days: int = 1,
        dimensions: List[str] = DIMENSIONS,
        metrics: List[str] = METRICS,
        ad_unit_view: str = AD_UNIT_VIEW,
    ) -> pd.DataFrame:
        """
        Get a report as a DataFrame, using a statement.

        Parameters
        ----------
        statement : googleads.ad_manager.StatementBuilder
            The statement for the report query.
        report_date : datetime.date, optional
            The end date for the report. Defaults to today.
        days : int, optional
            The number of days to include in the report. Defaults to 1.
        dimensions : list[str], optional
            The dimensions to include in the report. Defaults to ``DIMENSIONS``.
        metrics : list[str], optional
            The metrics to include in the report. Defaults to ``METRICS``.
        ad_unit_view : str, optional
            The ad unit view for the report. Defaults to ``AD_UNIT_VIEW``.

        Returns
        -------
        pandas.DataFrame
            The report data.

        Raises
        ------
        googleads.errors.AdManagerReportError
            If the report fails to generate.
        """
        report_job = self.gen_report_query(
            statement, report_date, days, dimensions, metrics, AdUnitView[ad_unit_view]
        )
        report_content = self._get_report_by_report_job(report_job)
        df = pd.DataFrame.from_dict(
            report_content,  # type: ignore
            orient="columns",
        )
        return ReportService.normalise_report(df)


class ForecastService:
    """A wrapper for the Forecast service of the Ad Manager API.

    Methods
    -------
    get_forecast(...)
        Get a forecast for a given set of targeting criteria.
    get_forecast_by_targeting_preset(...)
        Get a forecast by targeting presets.
    """

    _service_name = "ForecastService"
    _gam_service: Any

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the ForecastService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self._gam_service = gam_client.get_service(
            service_name=self._service_name, gam_version=gam_version
        )

    @staticmethod
    def _gen_line_item(
        targetedAdUnits: List[Dict[str, Any]],
        creativePlaceholders: List[CreativePlaceholder],
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        )
        + datetime.timedelta(days=1),
        days: int = 30,
    ) -> Dict[str, Any]:
        """Generate a prospective line item for a forecast."""
        logging.debug("Report::__gen_line_item")
        prospective_line_item = {
            "lineItem": {
                "targeting": {
                    "inventoryTargeting": {"targetedAdUnits": targetedAdUnits}
                },
                "creativePlaceholders": creativePlaceholders,
                "lineItemType": "STANDARD",
                "startDateTimeType": "IMMEDIATELY",
                "endDateTime": report_date + datetime.timedelta(days),
                "costType": "CPM",
                "costPerUnit": {"currencyCode": "USD", "microAmount": "2000000"},
                "primaryGoal": {
                    "units": "50",
                    "unitType": "IMPRESSIONS",
                    "goalType": "LIFETIME",
                },
                "creativeRotationType": "EVEN",
                "discountType": "PERCENTAGE",
            },
            "advertiserId": None,
        }
        return prospective_line_item

    @staticmethod
    def _gen_forecast_options(
        targets_list: List[Dict[str, Any]],
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        ),
        days: int = 30,
    ) -> Dict[str, Any]:
        """Generate forecast options."""
        logging.debug("Forecast::_gen_forecast_options")
        targets = [
            {
                "name": target.get("name"),
                "targeting": {
                    "customTargeting": {
                        "xsi_type": "CustomCriteriaSet",
                        "logicalOperator": "OR",
                        "children": {
                            "xsi_type": "CustomCriteria",
                            "keyId": target.get("keyId"),
                            "valueIds": [target.get("valueIds")],
                            "operator": "IS",
                        },
                    }
                },
            }
            for target in targets_list
        ]
        timeWindows = [report_date + datetime.timedelta(days=d) for d in range(days)]
        forecast_options = {
            "includeContendingLineItems": True,
            # The field includeTargetingCriteriaBreakdown can only be set if
            # breakdowns are not manually specified.
            # 'includeTargetingCriteriaBreakdown': True,
            "breakdown": {"timeWindows": timeWindows, "targets": targets},
        }
        return forecast_options

    @staticmethod
    def _gen_forecast_options_by_targeting_presets(
        targeting_presets: List[TargetingPreset],
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        ),
        days: int = 30,
    ) -> Dict[str, Any]:
        """Generate forecast options by targeting presets."""
        logging.debug("Forecast::_gen_forecast_options_by_targeting_presets")
        timeWindows = [report_date + datetime.timedelta(days=d) for d in range(days)]

        targets = [
            {
                "name": targeting_preset["name"],
                "targeting": {
                    "customTargeting": targeting_preset["targeting"]["customTargeting"]
                },
            }
            for targeting_preset in targeting_presets
        ]
        forecast_options = {
            "includeContendingLineItems": True,
            # The field includeTargetingCriteriaBreakdown can only be set if
            # breakdowns are not manually specified.
            # 'includeTargetingCriteriaBreakdown': True,
            "breakdown": {"timeWindows": timeWindows, "targets": targets},
        }
        return forecast_options

    def get_forecast(
        self,
        targetedAdUnits: List[Dict[str, Any]],
        creativePlaceholders: List[CreativePlaceholder],
        targets_list: List[Dict[str, Any]],
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        ),
        days: int = 30,
    ) -> List[ForecastItem]:
        """
        Get a forecast for a given set of targeting criteria.

        Parameters
        ----------
        targetedAdUnits : list[dict[str, Any]]
            The ad units to target.
        creativePlaceholders : list[CreativePlaceholder]
            The creative placeholders for the line item.
        targets_list : list[dict[str, Any]]
            The list of targeting criteria.
        report_date : datetime.datetime, optional
            The start date for the forecast. Defaults to now.
        days : int, optional
            The number of days to forecast. Defaults to 30.

        Returns
        -------
        list[ForecastItem]
            A list of forecast items.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("Forecast::get_forecast")
        # Create prospective line item.
        prospective_line_item = self._gen_line_item(
            targetedAdUnits, creativePlaceholders, report_date, days
        )

        forecast_options = self._gen_forecast_options(targets_list, report_date, days)

        # Get forecast.
        forecast = self._gam_service.getAvailabilityForecast(
            prospective_line_item, forecast_options
        )

        forecast_list: List[ForecastItem] = []
        if "breakdowns" in forecast and len(forecast["breakdowns"]):
            for breakdown in forecast["breakdowns"]:
                for breakdown_entry in breakdown["breakdownEntries"]:
                    value_date = breakdown["startTime"]["date"]
                    dt = datetime.date(
                        value_date["year"], value_date["month"], value_date["day"]
                    )
                    item: ForecastItem = {
                        "date": dt,
                        "matched": breakdown_entry["forecast"]["matched"],
                        "available": breakdown_entry["forecast"]["available"],
                        "possible": (
                            breakdown_entry["forecast"]["possible"]
                            if "possible" in breakdown_entry["forecast"]
                            else 0
                        ),
                        "name": (
                            breakdown_entry["name"] if "name" in breakdown_entry else ""
                        ),
                    }
                    forecast_list.append(item)

        return forecast_list

    def get_forecast_by_targeting_preset(
        self,
        targetedAdUnits: List[Dict[str, Any]],
        creativePlaceholders: List[CreativePlaceholder],
        targeting_presets: List[TargetingPreset],
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        ),
        days: int = 30,
    ) -> List[ForecastItem]:
        """
        Get a forecast by targeting presets.

        Parameters
        ----------
        targetedAdUnits : list[dict[str, Any]]
            The ad units to target.
        creativePlaceholders : list[CreativePlaceholder]
            The creative placeholders for the line item.
        targeting_presets : list[TargetingPreset]
            The targeting presets to use.
        report_date : datetime.datetime, optional
            The start date for the forecast. Defaults to now.
        days : int, optional
            The number of days to forecast. Defaults to 30.

        Returns
        -------
        list[ForecastItem]
            A list of forecast items.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("Forecast::get_forecast_by_targeting_preset")
        # Create prospective line item.
        prospective_line_item = self._gen_line_item(
            targetedAdUnits, creativePlaceholders, report_date, days
        )

        forecast_options = self._gen_forecast_options_by_targeting_presets(
            targeting_presets, report_date, days
        )

        # Get forecast.
        forecast = self._gam_service.getAvailabilityForecast(
            prospective_line_item, forecast_options
        )

        forecast_list: List[ForecastItem] = []
        if "breakdowns" in forecast and len(forecast["breakdowns"]):
            for breakdown in forecast["breakdowns"]:
                for breakdown_entry in breakdown["breakdownEntries"]:
                    value_date = breakdown["startTime"]["date"]
                    dt = datetime.date(
                        value_date["year"], value_date["month"], value_date["day"]
                    )
                    item: ForecastItem = {
                        "date": dt,
                        "matched": breakdown_entry["forecast"]["matched"],
                        "available": breakdown_entry["forecast"]["available"],
                        "possible": (
                            breakdown_entry["forecast"]["possible"]
                            if "possible" in breakdown_entry["forecast"]
                            else 0
                        ),
                        "name": (
                            breakdown_entry["name"] if "name" in breakdown_entry else ""
                        ),
                    }
                    forecast_list.append(item)
        return forecast_list


class TrafficService:
    """A wrapper for the Traffic service of the Ad Manager API.

    Methods
    -------
    get_traffic(...)
        Get traffic data for a given set of targeting criteria.
    get_traffic_by_targeting_preset(...)
        Get traffic data by targeting preset.
    """

    _service_name = "TrafficService"
    _gam_service: Any

    def __init__(
        self,
        app_name: str = APP_NAME,
        network_code: str = NETWORK_CODE,
        gam_version: str = GAM_VERSION,
    ) -> None:
        """
        Initialise the TrafficService.

        Parameters
        ----------
        app_name : str, optional
            The name of the application. Defaults to ``APP_NAME``.
        network_code : str, optional
            The Ad Manager network code. Defaults to ``NETWORK_CODE``.
        gam_version : str, optional
            The version of the API to use. Defaults to ``GAM_VERSION``.
        """
        gam_client = GamClient(app_name=app_name, network_code=network_code)
        self._gam_service = gam_client.get_service(
            service_name=self._service_name, gam_version=gam_version
        )

    def get_traffic(
        self,
        inventory_targeting: Optional[Dict[str, Any]] = None,
        custom_targeting: Optional[CustomCriteriaSet] = None,
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        ),
        days: int = 30,
    ) -> List[trafficItem]:
        """
        Get traffic data for a given set of targeting criteria.

        Parameters
        ----------
        inventory_targeting : dict[str, Any], optional
            The inventory targeting. Defaults to the root ad unit.
        custom_targeting : CustomCriteriaSet, optional
            The custom targeting. Defaults to ``None``.
        report_date : datetime.datetime, optional
            The start date for the traffic data. Defaults to now.
        days : int, optional
            The number of days of traffic data to get. Defaults to 30.

        Returns
        -------
        list[trafficItem]
            A list of traffic items.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("Traffic::get_traffic")

        def time_series_to_list(
            time_series: Dict[str, Any],
        ) -> List[trafficItem]:
            logging.debug("Traffic::time_series_to_list")
            date_range = time_series["timeSeriesDateRange"]
            time_series_start_date = datetime.date(
                date_range["startDate"]["year"],
                date_range["startDate"]["month"],
                date_range["startDate"]["day"],
            )
            time_series_end_date = datetime.date(
                date_range["endDate"]["year"],
                date_range["endDate"]["month"],
                date_range["endDate"]["day"],
            )
            time_series_forecast_data: List[trafficItem] = []
            offset = 0
            current_date = time_series_start_date
            while current_date <= time_series_end_date:
                time_series_forecast_data.append(
                    {"date": current_date, "impressions": time_series["values"][offset]}
                )
                offset += 1
                current_date = time_series_start_date + datetime.timedelta(days=offset)
            return time_series_forecast_data

        # the time-lapse to for forecast
        start_date = report_date.date() - datetime.timedelta(days=days)
        end_date = report_date.date() + datetime.timedelta(days=days)

        if inventory_targeting is None:
            inventory_targeting = {
                "targetedAdUnits": [
                    {"adUnitId": NetworkService().effective_root_ad_unit_id()}
                ]
            }

        # Create targeting.
        targeting = {
            "inventoryTargeting": inventory_targeting,
            "customTargeting": custom_targeting,
        }

        # Request the traffic forecast data.
        start = datetime.datetime.now()
        traffic_data = self._gam_service.getTrafficData(
            {
                "targeting": targeting,
                "requestedDateRange": {"startDate": start_date, "endDate": end_date},
            }
        )
        wait_time = 2 - (datetime.datetime.now() - start).total_seconds()
        if wait_time > 0:
            sleep(wait_time)
        historical_data = time_series_to_list(traffic_data["historicalTimeSeries"])
        forecasted_data = time_series_to_list(traffic_data["forecastedTimeSeries"])
        historical_data.extend(forecasted_data)
        return historical_data

    def get_traffic_by_targeting_preset(
        self,
        inventory_targeting: Dict[str, Any],
        targeting_preset: TargetingPreset,
        report_date: datetime.datetime = datetime.datetime.now(
            tz=pytz.timezone(PYTZ_TIMEZONE)
        ),
        days: int = 1,
    ) -> List[trafficItem]:
        """
        Get traffic data by targeting preset.

        Parameters
        ----------
        inventory_targeting : dict[str, Any]
            The inventory targeting.
        targeting_preset : TargetingPreset
            The targeting preset to use.
        report_date : datetime.datetime, optional
            The start date for the traffic data. Defaults to now.
        days : int, optional
            The number of days of traffic data to get. Defaults to 1.

        Returns
        -------
        list[trafficItem]
            A list of traffic items.

        Raises
        ------
        googleads.errors.GoogleAdsError
            If the API returns an error.
        """
        logging.debug("Traffic:get_traffic_by_targeting_preset")
        return self.get_traffic(
            inventory_targeting=inventory_targeting,
            custom_targeting=targeting_preset["targeting"]["customTargeting"],
            report_date=report_date,
            days=days,
        )
