from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AirflowConfig:
    base_url: str
    username: str
    password: str


def get_airflow_config() -> AirflowConfig:
    base_url = os.getenv(
        "AIRFLOW_BASE_URL",
        "http://localhost:8080",
    )

    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")

    if not username:
        raise RuntimeError("AIRFLOW_USERNAME environment variable is required.")

    if not password:
        raise RuntimeError("AIRFLOW_PASSWORD environment variable is required.")

    return AirflowConfig(
        base_url=base_url,
        username=username,
        password=password,
    )
