"""Public package exports for GoogleCloudPlatformAPI.

Service exports are resolved only when requested so independent integrations do
not import unrelated provider clients during package initialization.
"""

from importlib import import_module
import sys
from types import ModuleType
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
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return module attributes including lazy public exports."""
    return sorted(set(globals()) | set(__all__))


class _LazyExportModule(ModuleType):
    """Module type that preserves lazy class exports after direct module imports."""

    def __getattribute__(self, name: str) -> Any:
        """Resolve an exported class when its submodule shadows the package name."""
        module_name = _EXPORT_MODULES.get(name)
        if module_name is not None:
            current = ModuleType.__getattribute__(self, "__dict__").get(name)
            if isinstance(current, ModuleType):
                value = getattr(current, name)
                ModuleType.__setattr__(self, name, value)
                return value
            if current is None:
                return __getattr__(name)
        return ModuleType.__getattribute__(self, name)


sys.modules[__name__].__class__ = _LazyExportModule
