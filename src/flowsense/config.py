from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AirflowConfig:
    base_url: str
    username: str
    password: str
    api_version: str = "v2"
    auth_mode: str = "token"
    connect_timeout: float = 10.0
    read_timeout: float = 10.0
    max_retries: int = 2
    retry_backoff: float = 0.5
    history_run_limit: int = 100


def get_airflow_config() -> AirflowConfig:
    base_url = os.getenv(
        "AIRFLOW_BASE_URL",
        "http://localhost:8080",
    )

    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")
    api_version = os.getenv("AIRFLOW_API_VERSION", "v2")
    auth_mode = os.getenv("AIRFLOW_AUTH_MODE", "token")
    connect_timeout = float(os.getenv("AIRFLOW_CONNECT_TIMEOUT", "10"))
    read_timeout = float(os.getenv("AIRFLOW_READ_TIMEOUT", "10"))
    max_retries = int(os.getenv("AIRFLOW_MAX_RETRIES", "2"))
    retry_backoff = float(os.getenv("AIRFLOW_RETRY_BACKOFF", "0.5"))
    history_run_limit = int(os.getenv("AIRFLOW_HISTORY_RUN_LIMIT", "100"))

    if not username:
        raise RuntimeError("AIRFLOW_USERNAME environment variable is required.")

    if not password:
        raise RuntimeError("AIRFLOW_PASSWORD environment variable is required.")

    return AirflowConfig(
        base_url=base_url,
        username=username,
        password=password,
        api_version=api_version,
        auth_mode=auth_mode,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        history_run_limit=history_run_limit,
    )
