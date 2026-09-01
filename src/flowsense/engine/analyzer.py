"""Backward-compatible analyzer entry point.

New integrations should inject a data source into
``flowsense.application.analyze_dag``.
"""

from flowsense.application import analyze_dag as analyze_dag_with_source
from flowsense.domain import DAGAnalysis
from flowsense.infrastructure.airflow import AirflowClient


def analyze_dag(dag_id: str) -> DAGAnalysis:
    with AirflowClient() as source:
        return analyze_dag_with_source(
            dag_id=dag_id,
            source=source,
        )
