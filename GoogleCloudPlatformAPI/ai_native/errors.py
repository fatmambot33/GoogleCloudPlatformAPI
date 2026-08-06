"""Normalize provider and transport failures into stable capability errors."""

import re
from typing import Any, Dict, Optional, Tuple

from GoogleCloudPlatformAPI.ai_native.contracts import CapabilityError

_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization:\s*bearer\s+)[^\s,;]+"),
    re.compile(
        r"(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|password|secret)"
        r"\s*[=:]\s*)[^\s,;]+"
    ),
    re.compile(r"(?i)(credentials?\s+(?:file|path)\s*[=:]\s*)[^\s,;]+"),
)


class CapabilityTimeoutError(TimeoutError):
    """Indicate that a bounded capability execution exceeded its deadline."""


class CapabilityExecutionError(RuntimeError):
    """Carry one normalized error across an adapter or protocol boundary."""

    def __init__(self, error: CapabilityError) -> None:
        self.error = error
        super().__init__(error.message)


def sanitize_message(message: str, fallback: str = "Operation failed.") -> str:
    """Remove common secret values from an exception message."""
    value = message.strip() or fallback
    for pattern in _SECRET_PATTERNS:
        value = pattern.sub(r"\1[REDACTED]", value)
    return value[:1000]


def _classification(exc: Exception) -> Tuple[str, bool, Optional[str]]:
    """Classify common Google and transport exception names."""
    name = exc.__class__.__name__
    classifications = {
        "DefaultCredentialsError": (
            "authentication_failed",
            False,
            "Configure Application Default Credentials or a valid service account.",
        ),
        "RefreshError": (
            "authentication_failed",
            False,
            "Refresh or replace the configured Google credentials.",
        ),
        "Unauthorized": (
            "authentication_failed",
            False,
            "Authenticate with credentials valid for the requested project.",
        ),
        "Forbidden": (
            "permission_denied",
            False,
            "Grant the capability's documented IAM permissions.",
        ),
        "PermissionDenied": (
            "permission_denied",
            False,
            "Grant the capability's documented IAM permissions.",
        ),
        "NotFound": (
            "not_found",
            False,
            "Discover available resources and verify the identifier.",
        ),
        "BadRequest": (
            "invalid_request",
            False,
            "Correct the request using the capability schema and provider guidance.",
        ),
        "InvalidArgument": (
            "invalid_request",
            False,
            "Correct the request using the capability schema and provider guidance.",
        ),
        "TooManyRequests": (
            "quota_exceeded",
            True,
            "Retry with backoff or request additional Google Cloud quota.",
        ),
        "ResourceExhausted": (
            "quota_exceeded",
            True,
            "Retry with backoff or request additional Google Cloud quota.",
        ),
        "ServiceUnavailable": (
            "service_unavailable",
            True,
            "Retry with exponential backoff.",
        ),
        "InternalServerError": (
            "service_unavailable",
            True,
            "Retry with exponential backoff.",
        ),
        "DeadlineExceeded": (
            "timeout",
            True,
            "Reduce the operation scope or increase the bounded timeout.",
        ),
        "TimeoutError": (
            "timeout",
            True,
            "Reduce the operation scope or increase the bounded timeout.",
        ),
        "CapabilityTimeoutError": (
            "timeout",
            True,
            "Reduce the operation scope or increase the bounded timeout.",
        ),
    }
    return classifications.get(name, ("execution_failed", False, None))


def normalize_exception(
    exc: Exception, details: Optional[Dict[str, Any]] = None
) -> CapabilityError:
    """Convert an exception into a stable, redacted capability error."""
    if isinstance(exc, CapabilityExecutionError):
        return exc.error
    code, retryable, guidance = _classification(exc)
    safe_details: Dict[str, Any] = {"exception_type": exc.__class__.__name__}
    if details:
        safe_details.update(details)
    return CapabilityError(
        code=code,
        message=sanitize_message(str(exc)),
        retryable=retryable,
        guidance=guidance,
        details=safe_details,
    )
