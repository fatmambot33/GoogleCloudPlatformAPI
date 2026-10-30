"""Public package exports for GoogleCloudPlatformAPI.

Service modules are loaded only when their public symbols are accessed. This
keeps a light package import from initializing unrelated Google client stacks.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .exceptions import (
    AuthenticationError,
    ConfigurationError,
    GoogleCloudPlatformAPIError,
    ServiceError,
    TransportError,
)

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


_LAZY_EXPORTS = {
    "Analytics": (".Analytics", "Analytics"),
    "AudienceService": (".AdManager", "AudienceService"),
    "BigQuery": (".BigQuery", "BigQuery"),
    "ClientCredentials": (".Oauth", "ClientCredentials"),
    "CloudStorage": (".CloudStorage", "CloudStorage"),
    "CustomCriteria": (".AdManager", "CustomCriteria"),
    "CustomCriteriaSet": (".AdManager", "CustomCriteriaSet"),
    "CustomCriteriaSubSet": (".AdManager", "CustomCriteriaSubSet"),
    "CustomTargetingService": (".AdManager", "CustomTargetingService"),
    "KeyValuePair": (".AdManager", "KeyValuePair"),
    "NetworkService": (".AdManager", "NetworkService"),
    "Operator": (".AdManager", "Operator"),
    "ReportService": (".AdManager", "ReportService"),
    "ServiceAccount": (".Oauth", "ServiceAccount"),
    "Targeting": (".AdManager", "Targeting"),
    "TargetingPreset": (".AdManager", "TargetingPreset"),
    "TargetingPresetService": (".AdManager", "TargetingPresetService"),
    "TrafficService": (".AdManager", "TrafficService"),
}

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


def __getattr__(name: str) -> Any:
    """Load a public service symbol on first access."""
    try:
        module_name, attribute_name = _LAZY_EXPORTS[name]
    except KeyError:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from None

    value = getattr(import_module(module_name, __name__), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return the complete public package surface for interactive tooling."""
    return sorted(set(globals()) | set(__all__))
