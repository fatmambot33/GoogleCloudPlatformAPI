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
from GoogleCloudPlatformAPI.ai_native.schema import SchemaValidationError

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
                "[REDACTED]" if str(key).lower() in _REDACTED_KEYS else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


def _result(
    registry: CapabilityRegistry,
    name: str,
    request_id: str,
    started: float,
    data: Any = None,
    error: Optional[CapabilityError] = None,
) -> CapabilityResult:
    """Build one result envelope with consistent duration metadata."""
    capability = registry.get(name)
    duration_ms = int(round((time.monotonic() - started) * 1000))
    return CapabilityResult(
        ok=error is None,
        data=data,
        metadata=ResultMetadata(
            request_id=request_id,
            service=capability.service,
            operation=capability.operation,
            duration_ms=duration_ms,
        ),
        error=error,
    )


def execute_capability(
    registry: CapabilityRegistry,
    name: str,
    arguments: Dict[str, Any],
    handler: Optional[Callable[..., Any]] = None,
) -> CapabilityResult:
    """Validate and execute one capability with a stable result envelope."""
    capability = registry.get(name)
    selected_handler = handler or capability.handler
    request_id = str(uuid.uuid4())
    started = time.monotonic()

    try:
        registry.validate_input(name, arguments)
    except SchemaValidationError as exc:
        return _result(
            registry,
            name,
            request_id,
            started,
            error=CapabilityError(
                code="input_validation_failed",
                message=str(exc),
                retryable=False,
                guidance="Correct the arguments using the capability input schema.",
            ),
        )

    if selected_handler is None:
        return _result(
            registry,
            name,
            request_id,
            started,
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
        registry.validate_output(name, data)
        error = None
    except SchemaValidationError as exc:
        data = None
        error = CapabilityError(
            code="output_validation_failed",
            message=str(exc),
            retryable=False,
            guidance="Fix the adapter output to match the capability contract.",
        )
    except Exception as exc:  # pragma: no cover - adapter boundary
        data = None
        error = CapabilityError(
            code=exc.__class__.__name__,
            message=str(exc),
            retryable=False,
        )

    result = _result(registry, name, request_id, started, data=data, error=error)
    _LOGGER.info(
        "capability.finish",
        extra={
            "capability": name,
            "request_id": request_id,
            "duration_ms": result.metadata.duration_ms,
            "success": result.ok,
        },
    )
    return result
