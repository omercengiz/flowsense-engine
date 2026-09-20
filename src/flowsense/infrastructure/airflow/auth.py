from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

import httpx


@dataclass(frozen=True)
class AirflowRequestAuth:
    """Authentication material applied to one Airflow API request."""

    headers: Mapping[str, str]
    auth: httpx.Auth | None = None


class AirflowAuthProvider(Protocol):
    """Provide request authentication independently from Airflow collection."""

    def request_auth(self) -> AirflowRequestAuth: ...


class BasicAuthProvider:
    def __init__(self, username: str, password: str) -> None:
        if not username or not password:
            raise ValueError("basic authentication requires username and password")
        self._auth = httpx.BasicAuth(username, password)

    def request_auth(self) -> AirflowRequestAuth:
        return AirflowRequestAuth(headers={}, auth=self._auth)


class BearerTokenAuthProvider:
    def __init__(self, token: str | Callable[[], str]) -> None:
        self._token = token

    def request_auth(self) -> AirflowRequestAuth:
        token = self._token() if callable(self._token) else self._token
        if not token:
            raise ValueError("bearer token must not be empty")
        return AirflowRequestAuth(headers={"Authorization": f"Bearer {token}"})


class HeaderAuthProvider:
    """Apply deployment-specific authentication headers to Airflow requests."""

    def __init__(self, headers: Mapping[str, str]) -> None:
        if not headers:
            raise ValueError("authentication headers must not be empty")
        validated: dict[str, str] = {}
        for name, value in headers.items():
            if not name or not value:
                raise ValueError(
                    "authentication header names and values must not be empty"
                )
            if "\n" in name or "\r" in name or "\n" in value or "\r" in value:
                raise ValueError("authentication headers must not contain newlines")
            validated[name] = value
        self._headers = validated

    def request_auth(self) -> AirflowRequestAuth:
        return AirflowRequestAuth(headers=dict(self._headers))
