"""Optional OpenTelemetry hooks with a dependency-free fallback."""

from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional


def _otel() -> Optional[Any]:
    """Return the OpenTelemetry API when its optional extra is installed."""
    try:
        from opentelemetry import metrics, trace
    except ImportError:
        return None
    return trace, metrics


@contextmanager
def capability_span(name: str, attributes: Dict[str, Any]) -> Iterator[Any]:
    """Create an optional trace span without requiring telemetry dependencies."""
    api = _otel()
    if api is None:
        yield None
        return
    trace, _ = api
    tracer = trace.get_tracer("GoogleCloudPlatformAPI.ai_native")
    with tracer.start_as_current_span(name, attributes=attributes) as span:
        yield span


def record_execution(
    capability: str, duration_ms: int, success: bool, retry_count: int = 0
) -> None:
    """Record optional execution metrics when OpenTelemetry is installed."""
    api = _otel()
    if api is None:
        return
    _, metrics = api
    meter = metrics.get_meter("GoogleCloudPlatformAPI.ai_native")
    attributes = {"capability": capability, "success": success}
    meter.create_counter("gcp_api.capability.executions").add(1, attributes)
    meter.create_histogram("gcp_api.capability.duration_ms").record(
        duration_ms, attributes
    )
    if retry_count:
        meter.create_counter("gcp_api.capability.retries").add(
            retry_count, {"capability": capability}
        )
