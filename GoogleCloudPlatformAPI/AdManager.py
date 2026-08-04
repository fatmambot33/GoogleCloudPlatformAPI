"""Google Ad Manager helpers using the v202602 SOAP API by default.

The implementation remains in :mod:`._AdManagerLegacy` to preserve backwards
compatibility while this module centralises the supported API version.
"""

from . import _AdManagerLegacy as _implementation
from ._AdManagerLegacy import *  # noqa: F401,F403

GAM_VERSION = "v202602"
_implementation.GAM_VERSION = GAM_VERSION

for _service_class in (
    AudienceService,
    NetworkService,
    CustomTargetingService,
    TargetingPresetService,
    ReportService,
    ForecastService,
    TrafficService,
):
    _defaults = _service_class.__init__.__defaults__
    if _defaults:
        _service_class.__init__.__defaults__ = (*_defaults[:-1], GAM_VERSION)

del _defaults
del _service_class
