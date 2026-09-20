from collections.abc import Iterator
from contextlib import contextmanager
from threading import Barrier
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
    FlowSenseError,
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


def test_client_analyze_many_preserves_order_and_isolates_expected_failures() -> None:
    @contextmanager
    def source_factory(request: AnalysisRequest) -> Iterator[DAGDataSource]:
        if request.dag_id == "broken":
            raise ConfigurationError("broken DAG configuration")
        source = MagicMock(spec=DAGDataSource)
        source.collect_task_runs.return_value = []
        source.get_dag_dependencies.return_value = {}
        yield source

    result = FlowSenseClient(source_factory).analyze_many(["first", "broken", "last"])

    assert result.requested_dag_ids == ("first", "broken", "last")
    assert list(result.analyses) == ["first", "last"]
    assert list(result.failures) == ["broken"]
    assert result.successful_count == 2
    assert result.failed_count == 1
    assert result.is_successful is False
    assert isinstance(result.failures["broken"], FlowSenseError)


def test_client_analyze_many_supports_bounded_concurrency() -> None:
    barrier = Barrier(2)

    @contextmanager
    def source_factory(_request: AnalysisRequest) -> Iterator[DAGDataSource]:
        barrier.wait(timeout=2)
        source = MagicMock(spec=DAGDataSource)
        source.collect_task_runs.return_value = []
        source.get_dag_dependencies.return_value = {}
        yield source

    result = FlowSenseClient(source_factory).analyze_many(
        ["first", "second"],
        max_concurrency=2,
    )

    assert list(result.analyses) == ["first", "second"]
    assert result.is_successful is True


@pytest.mark.parametrize(
    ("dag_ids", "max_concurrency", "message"),
    [
        ([], 1, "at least one"),
        (["demo", "demo"], 1, "duplicates"),
        (["demo"], 0, "max_concurrency"),
        (["  "], 1, "dag_id must not be empty"),
    ],
)
def test_client_analyze_many_validates_batch_input(
    dag_ids: list[str],
    max_concurrency: int,
    message: str,
) -> None:
    client = FlowSenseClient(MagicMock())

    with pytest.raises(ConfigurationError, match=message):
        client.analyze_many(dag_ids, max_concurrency=max_concurrency)


def test_client_analyze_many_rejects_invalid_shared_history_limit() -> None:
    client = FlowSenseClient(MagicMock())

    with pytest.raises(ConfigurationError, match="minimum_history"):
        client.analyze_many(
            ["first", "second"],
            policy=AnalysisPolicy(minimum_history=10),
            history_run_limit=5,
        )


def test_client_analyze_many_does_not_hide_unexpected_errors() -> None:
    @contextmanager
    def source_factory(_request: AnalysisRequest) -> Iterator[DAGDataSource]:
        raise RuntimeError("unexpected adapter defect")
        yield MagicMock(spec=DAGDataSource)

    with pytest.raises(RuntimeError, match="unexpected adapter defect"):
        FlowSenseClient(source_factory).analyze_many(["demo"])
