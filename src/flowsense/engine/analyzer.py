"""Backward-compatible analyzer entry point.

New integrations should inject a data source into
``flowsense.application.analyze_dag``.
"""

from flowsense.application import AnalysisRequest, AnalyzeDAG
from flowsense.domain import DAGAnalysis
from flowsense.infrastructure.airflow import create_airflow_data_source


def analyze_dag(dag_id: str) -> DAGAnalysis:
    return AnalyzeDAG(create_airflow_data_source).execute(
        AnalysisRequest(dag_id=dag_id)
    )
