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


def get_airflow_config() -> AirflowConfig:
    base_url = os.getenv(
        "AIRFLOW_BASE_URL",
        "http://localhost:8080",
    )

    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")
    api_version = os.getenv("AIRFLOW_API_VERSION", "v2")
    auth_mode = os.getenv("AIRFLOW_AUTH_MODE", "token")

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
    )
