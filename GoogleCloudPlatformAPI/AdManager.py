"""Public Ad Manager helpers with single-resource retrieval methods."""

from typing import Any, Dict, Optional

from googleads import ad_manager

from ._ad_manager_core import (
    AD_UNIT_VIEW,
    APP_NAME,
    DIMENSIONS,
    GAM_VERSION,
    METRICS,
    NETWORK_CODE,
    PYTZ_TIMEZONE,
    AdUnitView,
    AudienceService as _AudienceServiceCore,
    CreativePlaceholder,
    CustomCriteria,
    CustomCriteriaSet,
    CustomCriteriaSubSet,
    CustomTargetingService as _CustomTargetingServiceCore,
    ForecastItem,
    ForecastService,
    GamClient,
    KeyValuePair,
    NetworkService,
    Operator,
    ReportService,
    Size,
    Targeting,
    TargetingPreset,
    TargetingPresetService as _TargetingPresetServiceCore,
    TrafficService,
    trafficItem,
)


class AudienceService(_AudienceServiceCore):
    """Audience service with a first-class single-segment read."""

    def get(self, audience_segment_id: int) -> Optional[Dict[str, Any]]:
        """Return one first-party audience segment by ID.

        Parameters
        ----------
        audience_segment_id : int
            Audience segment identifier.

        Returns
        -------
        dict[str, Any] or None
            The matching audience segment, or ``None`` when it is absent.
        """
        statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("Type = :type AND Id = :audience_segment_id")
            .WithBindVariable("type", "FIRST_PARTY")
            .WithBindVariable("audience_segment_id", int(audience_segment_id))
            .Limit(1)
        )
        response = self._gam_service.getAudienceSegmentsByStatement(
            statement.ToStatement()
        )
        results = response.get("results", []) if response else []
        return results[0] if results else None


class CustomTargetingService(_CustomTargetingServiceCore):
    """Custom targeting service with a first-class value read."""

    def get(
        self, targeting_key_id: int, targeting_value_id: int
    ) -> Optional[KeyValuePair]:
        """Return one custom-targeting value by key and value ID.

        Parameters
        ----------
        targeting_key_id : int
            Custom-targeting key identifier.
        targeting_value_id : int
            Custom-targeting value identifier.

        Returns
        -------
        KeyValuePair or None
            The matching value, or ``None`` when it is absent.
        """
        statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("customTargetingKeyId = :key_id AND id = :value_id")
            .WithBindVariable("key_id", int(targeting_key_id))
            .WithBindVariable("value_id", int(targeting_value_id))
            .Limit(1)
        )
        response = self._gam_service.getCustomTargetingValuesByStatement(
            statement.ToStatement()
        )
        results = response.get("results", []) if response else []
        return results[0] if results else None


class TargetingPresetService(_TargetingPresetServiceCore):
    """Targeting preset service with a first-class single-preset read."""

    def get(self, targeting_preset_id: int) -> Optional[TargetingPreset]:
        """Return one targeting preset by ID.

        Parameters
        ----------
        targeting_preset_id : int
            Targeting preset identifier.

        Returns
        -------
        TargetingPreset or None
            The matching preset, or ``None`` when it is absent.
        """
        statement = (
            ad_manager.StatementBuilder(version=GAM_VERSION)
            .Where("id = :targeting_preset_id")
            .WithBindVariable("targeting_preset_id", int(targeting_preset_id))
            .Limit(1)
        )
        response = self._gam_service.getTargetingPresetsByStatement(
            statement.ToStatement()
        )
        results = response.get("results", []) if response else []
        return results[0] if results else None


__all__ = [
    "AD_UNIT_VIEW",
    "APP_NAME",
    "DIMENSIONS",
    "GAM_VERSION",
    "METRICS",
    "NETWORK_CODE",
    "PYTZ_TIMEZONE",
    "AdUnitView",
    "AudienceService",
    "CreativePlaceholder",
    "CustomCriteria",
    "CustomCriteriaSet",
    "CustomCriteriaSubSet",
    "CustomTargetingService",
    "ForecastItem",
    "ForecastService",
    "GamClient",
    "KeyValuePair",
    "NetworkService",
    "Operator",
    "ReportService",
    "Size",
    "Targeting",
    "TargetingPreset",
    "TargetingPresetService",
    "TrafficService",
    "trafficItem",
]
