"""Safe execution helpers for AI-facing capabilities."""

import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional

from GoogleCloudPlatformAPI.ai_native.contracts import (
    CapabilityError,
    CapabilityResult,
    ResultMetadata,
)
from GoogleCloudPlatformAPI.ai_native.registry import CapabilityRegistry

_LOGGER = logging.getLogger("GoogleCloudPlatformAPI.ai_native")
_REDACTED_KEYS = {
    "authorization",
    "credential",
    "credentials",
    "password",
    "secret",
    "token",
}


def redact(value: Any) -> Any:
    """Recursively redact common secret-bearing mapping keys."""
    if isinstance(value, dict):
        return {
            str(key): (
                "[REDACTED]"
                if str(key).lower() in _REDACTED_KEYS
                else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


def execute_capability(
    registry: CapabilityRegistry,
    name: str,
    arguments: Dict[str, Any],
    handler: Optional[Callable[..., Any]] = None,
) -> CapabilityResult:
    """Execute one registered capability and return a stable result envelope."""
    capability = registry.get(name)
    selected_handler = handler or capability.handler
    request_id = str(uuid.uuid4())
    started = time.monotonic()
    if selected_handler is None:
        return CapabilityResult(
            data=None,
            metadata=ResultMetadata(
                request_id=request_id,
                service=capability.service,
                operation=capability.operation,
            ),
            error=CapabilityError(
                code="handler_unavailable",
                message="No execution handler is configured for this capability.",
                retryable=False,
            ),
        )
    _LOGGER.info(
        "capability.start",
        extra={
            "capability": name,
            "request_id": request_id,
            "arguments": redact(arguments),
        },
    )
    try:
        data = selected_handler(**arguments)
        error = None
    except Exception as exc:  # pragma: no cover - adapter boundary
        data = None
        error = CapabilityError(
            code=exc.__class__.__name__,
            message=str(exc),
            retryable=False,
        )
    duration_ms = round((time.monotonic() - started) * 1000, 3)
    metadata = ResultMetadata(
        request_id=request_id,
        service=capability.service,
        operation=capability.operation,
        duration_ms=duration_ms,
    )
    _LOGGER.info(
        "capability.finish",
        extra={
            "capability": name,
            "request_id": request_id,
            "duration_ms": duration_ms,
            "success": error is None,
        },
    )
    return CapabilityResult(data=data, metadata=metadata, error=error)
