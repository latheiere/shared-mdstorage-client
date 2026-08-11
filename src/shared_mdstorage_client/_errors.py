from __future__ import annotations


class StorageClientError(Exception):
    """Base class for storage client failures."""


class TransportError(StorageClientError):
    """The request did not complete at the HTTP transport boundary."""

    def __init__(self, message: str, *, cause: Exception) -> None:
        super().__init__(message)
        self.cause = cause


class ProtocolError(StorageClientError):
    """The service returned an invalid or unsuccessful protocol response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        method: str | None = None,
        url: str | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.method = method
        self.url = url
        self.response_body = response_body


class ConflictError(ProtocolError):
    """A conditional or concurrent write could not be applied."""


class NotFoundError(ProtocolError):
    """The requested storage resource does not exist."""


class AuthenticationError(ProtocolError):
    """The request is not authenticated or authorized."""
