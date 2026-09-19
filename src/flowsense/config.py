"""Backward-compatible configuration imports.

New integrations should import Airflow configuration from
``flowsense.infrastructure.airflow``.
"""

import warnings

from flowsense.infrastructure.airflow.config import (
    AirflowConfig,
    load_airflow_config,
)

get_airflow_config = load_airflow_config

warnings.warn(
    "flowsense.config is deprecated; use flowsense.infrastructure.airflow instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["AirflowConfig", "get_airflow_config", "load_airflow_config"]
