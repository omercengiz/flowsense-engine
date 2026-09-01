"""Backward-compatible imports for the Airflow infrastructure adapter."""

from flowsense.infrastructure.airflow.client import PAGE_SIZE, AirflowClient

__all__ = ["PAGE_SIZE", "AirflowClient"]
