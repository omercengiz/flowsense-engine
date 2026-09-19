from collections.abc import Callable
from contextlib import AbstractContextManager

from flowsense.application.analyzer import analyze_dag
from flowsense.application.ports import DAGDataSource
from flowsense.application.request import AnalysisRequest
from flowsense.domain import DAGAnalysis

DAGDataSourceFactory = Callable[
    [AnalysisRequest],
    AbstractContextManager[DAGDataSource],
]


class AnalyzeDAG:
    """Application use case coordinating data collection and DAG analysis."""

    def __init__(self, source_factory: DAGDataSourceFactory) -> None:
        self._source_factory = source_factory

    def execute(self, request: AnalysisRequest) -> DAGAnalysis:
        with self._source_factory(request) as source:
            return analyze_dag(
                dag_id=request.dag_id,
                source=source,
                policy=request.policy,
            )
