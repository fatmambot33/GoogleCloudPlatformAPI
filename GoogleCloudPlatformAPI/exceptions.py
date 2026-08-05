"""Stable public exception hierarchy for GoogleCloudPlatformAPI."""

from typing import Any, Dict, Optional


class GoogleCloudPlatformAPIError(Exception):
    """Base class for package-level operational failures.

    Parameters
    ----------
    message : str
        Human-readable failure description.
    operation : str, optional
        Stable operation identifier associated with the failure.
    details : dict, optional
        JSON-compatible diagnostic details that do not contain secrets.
    """

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.operation = operation
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation of the failure."""
        return {
            "type": self.__class__.__name__,
            "message": self.message,
            "operation": self.operation,
            "details": self.details,
        }


class AuthenticationError(GoogleCloudPlatformAPIError):
    """Raised when Google credentials are missing, invalid, or rejected."""


class ConfigurationError(GoogleCloudPlatformAPIError):
    """Raised when required package or service configuration is invalid."""


class TransportError(GoogleCloudPlatformAPIError):
    """Raised when a request cannot reach or complete against Google APIs."""


class ServiceError(GoogleCloudPlatformAPIError):
    """Raised when a Google service rejects an otherwise valid operation."""
