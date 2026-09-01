"""Backward-compatible analyzer entry point.

New integrations should inject a data source into
``flowsense.application.analyze_dag``.
"""

from flowsense.application import analyze_dag as analyze_dag_with_source
from flowsense.collector.airflow_client import AirflowClient
from flowsense.domain import DAGAnalysis


def analyze_dag(dag_id: str) -> DAGAnalysis:
    return analyze_dag_with_source(
        dag_id=dag_id,
        source=AirflowClient(),
    )
