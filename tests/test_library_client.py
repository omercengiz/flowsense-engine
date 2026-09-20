from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from flowsense import (
    AnalysisPolicy,
    AnalysisRequest,
    ConfigurationError,
    DAGAnalysis,
    DAGAnalysisEngine,
    DAGDataSource,
    FlowSenseClient,
)


def _analysis(dag_id: str) -> DAGAnalysis:
    return DAGAnalysis(
        dag_id=dag_id,
        runs_analyzed=0,
        overall_severity="NORMAL",
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )


def test_client_analyze_builds_request_and_executes_analysis() -> None:
    source = MagicMock(spec=DAGDataSource)
    source.collect_task_runs.return_value = []
    source.get_dag_dependencies.return_value = {}
    requests: list[AnalysisRequest] = []

    @contextmanager
    def source_factory(request: AnalysisRequest) -> Iterator[DAGDataSource]:
        requests.append(request)
        yield source

    policy = AnalysisPolicy(minimum_history=3)
    client = FlowSenseClient(source_factory)

    result = client.analyze(
        "demo",
        policy=policy,
        history_run_limit=20,
        dag_run_id="run_42",
    )

    assert result.dag_id == "demo"
    assert requests == [
        AnalysisRequest(
            dag_id="demo",
            policy=policy,
            history_run_limit=20,
            dag_run_id="run_42",
        )
    ]


def test_client_execute_accepts_typed_request_and_custom_engine() -> None:
    source = MagicMock(spec=DAGDataSource)
    engine = MagicMock(spec=DAGAnalysisEngine)
    engine.analyze.return_value = _analysis("demo")

    @contextmanager
    def source_factory(_request: AnalysisRequest) -> Iterator[DAGDataSource]:
        source.collect_task_runs.return_value = []
        source.get_dag_dependencies.return_value = {}
        yield source

    request = AnalysisRequest(dag_id="demo")
    result = FlowSenseClient(source_factory, analysis_engine=engine).execute(request)

    assert result is engine.analyze.return_value


def test_client_analyze_preserves_request_validation() -> None:
    client = FlowSenseClient(MagicMock())

    with pytest.raises(ConfigurationError, match="dag_id must not be empty"):
        client.analyze("  ")
