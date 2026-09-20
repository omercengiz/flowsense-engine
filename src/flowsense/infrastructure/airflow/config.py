from __future__ import annotations

import os
from dataclasses import dataclass, field

from flowsense.domain import ConfigurationError


@dataclass(frozen=True)
class AirflowConfig:
    base_url: str
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    api_version: str = "v2"
    auth_mode: str = "token"
    bearer_token: str | None = field(default=None, repr=False)
    connect_timeout: float = 10.0
    read_timeout: float = 10.0
    max_retries: int = 2
    retry_backoff: float = 0.5
    history_run_limit: int = 100


def load_airflow_config() -> AirflowConfig:
    """Build Airflow infrastructure configuration from the environment."""
    base_url = os.getenv(
        "AIRFLOW_BASE_URL",
        "http://localhost:8080",
    )

    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")
    api_version = os.getenv("AIRFLOW_API_VERSION", "v2")
    auth_mode = os.getenv("AIRFLOW_AUTH_MODE", "token")
    bearer_token = os.getenv("AIRFLOW_BEARER_TOKEN")
    connect_timeout = _read_float("AIRFLOW_CONNECT_TIMEOUT", "10")
    read_timeout = _read_float("AIRFLOW_READ_TIMEOUT", "10")
    max_retries = _read_int("AIRFLOW_MAX_RETRIES", "2")
    retry_backoff = _read_float("AIRFLOW_RETRY_BACKOFF", "0.5")
    history_run_limit = _read_int("AIRFLOW_HISTORY_RUN_LIMIT", "100")

    if auth_mode in {"basic", "token"}:
        if not username:
            raise ConfigurationError(
                "AIRFLOW_USERNAME environment variable is required."
            )
        if not password:
            raise ConfigurationError(
                "AIRFLOW_PASSWORD environment variable is required."
            )
    elif auth_mode == "bearer":
        if not bearer_token:
            raise ConfigurationError(
                "AIRFLOW_BEARER_TOKEN environment variable is required."
            )
    else:
        raise ConfigurationError(
            "AIRFLOW_AUTH_MODE must be 'basic', 'token', or 'bearer'."
        )

    return AirflowConfig(
        base_url=base_url,
        username=username,
        password=password,
        api_version=api_version,
        auth_mode=auth_mode,
        bearer_token=bearer_token,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        history_run_limit=history_run_limit,
    )


def _read_float(name: str, default: str) -> float:
    value = os.getenv(name, default)
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number.") from exc


def _read_int(name: str, default: str) -> int:
    value = os.getenv(name, default)
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer.") from exc
