"""Deprecated compatibility namespace for Airflow collection."""

import warnings

warnings.warn(
    "flowsense.collector is deprecated; use flowsense.infrastructure.airflow instead.",
    DeprecationWarning,
    stacklevel=2,
)
