from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

from flowsense.application import (
    AnalysisRequest,
    AnalyzeDAG,
    DAGAnalysisEngine,
    DAGDataSource,
)
from flowsense.domain import DAGAnalysis


def test_analyze_dag_use_case_coordinates_request_and_data_source() -> None:
    source = MagicMock(spec=DAGDataSource)
    source.collect_task_runs.return_value = []
    source.get_dag_dependencies.return_value = {}
    received_requests: list[AnalysisRequest] = []

    @contextmanager
    def source_factory(
        request: AnalysisRequest,
    ) -> Iterator[DAGDataSource]:
        received_requests.append(request)
        yield source

    request = AnalysisRequest(dag_id="demo")

    analysis = AnalyzeDAG(source_factory).execute(request)

    assert received_requests == [request]
    assert analysis.dag_id == "demo"
    source.collect_task_runs.assert_called_once_with("demo")
    source.get_dag_dependencies.assert_called_once_with("demo")


def test_analyze_dag_use_case_accepts_an_alternative_analysis_engine() -> None:
    source = MagicMock(spec=DAGDataSource)
    source.collect_task_runs.return_value = []
    source.get_dag_dependencies.return_value = {"extract": []}
    engine = MagicMock(spec=DAGAnalysisEngine)
    engine.analyze.return_value = DAGAnalysis(
        dag_id="demo",
        runs_analyzed=0,
        overall_severity="NORMAL",
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={"extract": []},
    )

    @contextmanager
    def source_factory(_request: AnalysisRequest) -> Iterator[DAGDataSource]:
        yield source

    request = AnalysisRequest(dag_id="demo")

    analysis = AnalyzeDAG(source_factory, analysis_engine=engine).execute(request)

    assert analysis is engine.analyze.return_value
    engine.analyze.assert_called_once_with(
        dag_id="demo",
        task_runs=[],
        dependencies={"extract": []},
        policy=request.policy,
    )
