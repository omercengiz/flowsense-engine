"""Backward-compatible analyzer entry point.

New integrations should inject a data source into
``flowsense.application.analyze_dag``.
"""

import warnings

from flowsense.application import AnalysisRequest, AnalyzeDAG
from flowsense.domain import DAGAnalysis
from flowsense.infrastructure.airflow import create_airflow_data_source

warnings.warn(
    "flowsense.engine.analyzer is deprecated; use flowsense.AnalyzeDAG instead.",
    DeprecationWarning,
    stacklevel=2,
)


def analyze_dag(dag_id: str) -> DAGAnalysis:
    return AnalyzeDAG(create_airflow_data_source).execute(
        AnalysisRequest(dag_id=dag_id)
    )
