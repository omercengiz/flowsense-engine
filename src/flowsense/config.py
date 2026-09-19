"""Backward-compatible configuration imports.

New integrations should import Airflow configuration from
``flowsense.infrastructure.airflow``.
"""

from flowsense.infrastructure.airflow.config import (
    AirflowConfig,
    load_airflow_config,
)

get_airflow_config = load_airflow_config

__all__ = ["AirflowConfig", "get_airflow_config", "load_airflow_config"]
