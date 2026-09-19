from collections.abc import Callable
from contextlib import AbstractContextManager

from flowsense.application.analysis_engine import DEFAULT_DAG_ANALYSIS_ENGINE
from flowsense.application.ports import DAGAnalysisEngine, DAGDataSource
from flowsense.application.request import AnalysisRequest
from flowsense.domain import DAGAnalysis

DAGDataSourceFactory = Callable[
    [AnalysisRequest],
    AbstractContextManager[DAGDataSource],
]


class AnalyzeDAG:
    """Application use case coordinating data collection and DAG analysis."""

    def __init__(
        self,
        source_factory: DAGDataSourceFactory,
        analysis_engine: DAGAnalysisEngine = DEFAULT_DAG_ANALYSIS_ENGINE,
    ) -> None:
        self._source_factory = source_factory
        self._analysis_engine = analysis_engine

    def execute(self, request: AnalysisRequest) -> DAGAnalysis:
        with self._source_factory(request) as source:
            task_runs = source.collect_task_runs(request.dag_id)
            dependencies = source.get_dag_dependencies(request.dag_id)

            return self._analysis_engine.analyze(
                dag_id=request.dag_id,
                task_runs=task_runs,
                dependencies=dependencies,
                policy=request.policy,
            )
