"""Public package exports for GoogleCloudPlatformAPI.

Service exports are resolved only when requested so independent integrations do
not import unrelated provider clients during package initialization.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any, Dict

from .exceptions import (
    AuthenticationError,
    ConfigurationError,
    GoogleCloudPlatformAPIError,
    ServiceError,
    TransportError,
)

__all__ = [
    "Analytics",
    "AudienceService",
    "AuthenticationError",
    "BigQuery",
    "ClientCredentials",
    "CloudStorage",
    "ConfigurationError",
    "CustomCriteria",
    "CustomCriteriaSet",
    "CustomCriteriaSubSet",
    "CustomTargetingService",
    "GoogleCloudPlatformAPIError",
    "KeyValuePair",
    "NetworkService",
    "Operator",
    "ReportService",
    "ServiceAccount",
    "ServiceError",
    "Targeting",
    "TargetingPreset",
    "TargetingPresetService",
    "TrafficService",
    "TransportError",
]

_EXPORT_MODULES: Dict[str, str] = {
    "Analytics": ".Analytics",
    "AudienceService": ".AdManager",
    "BigQuery": ".BigQuery",
    "ClientCredentials": ".Oauth",
    "CloudStorage": ".CloudStorage",
    "CustomCriteria": ".AdManager",
    "CustomCriteriaSet": ".AdManager",
    "CustomCriteriaSubSet": ".AdManager",
    "CustomTargetingService": ".AdManager",
    "KeyValuePair": ".AdManager",
    "NetworkService": ".AdManager",
    "Operator": ".AdManager",
    "ReportService": ".AdManager",
    "ServiceAccount": ".Oauth",
    "Targeting": ".AdManager",
    "TargetingPreset": ".AdManager",
    "TargetingPresetService": ".AdManager",
    "TrafficService": ".AdManager",
}

if TYPE_CHECKING:
    from .AdManager import (
        AudienceService,
        CustomCriteria,
        CustomCriteriaSet,
        CustomCriteriaSubSet,
        CustomTargetingService,
        KeyValuePair,
        NetworkService,
        Operator,
        ReportService,
        Targeting,
        TargetingPreset,
        TargetingPresetService,
        TrafficService,
    )
    from .Analytics import Analytics
    from .BigQuery import BigQuery
    from .CloudStorage import CloudStorage
    from .Oauth import ClientCredentials, ServiceAccount


def __getattr__(name: str) -> Any:
    """Resolve a declared public service export when it is first accessed."""
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from None

    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return module attributes including lazy public exports."""
    return sorted(set(globals()) | set(__all__))
