from __future__ import annotations

import threading
from urllib.request import urlopen

import pytest

from flowsense import ConfigurationError, DAGAnalysis, DriftResult, Severity
from flowsense.application import AnalysisRequest
from flowsense.observability.prometheus import (
    CONTENT_TYPE,
    PrometheusExporter,
    create_metrics_server,
)
from flowsense.observability.service import collect_metrics_once


def _analysis(dag_id: str = "demo") -> DAGAnalysis:
    return DAGAnalysis(
        dag_id=dag_id,
        runs_analyzed=5,
        overall_severity=Severity.HIGH,
        primary_origin=None,
        drift_results={
            "extract": DriftResult(
                task_id="extract",
                baseline=1.0,
                current=1.0,
                mad=0.0,
                robust_z_score=0.0,
                deviation_percent=0.0,
                severity=Severity.NORMAL,
            ),
            "transform": DriftResult(
                task_id="transform",
                baseline=2.0,
                current=4.0,
                mad=0.5,
                robust_z_score=4.0,
                deviation_percent=100.0,
                severity=Severity.HIGH,
            ),
        },
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={"extract": ["transform"], "transform": []},
        current_dag_run_id="run_5",
    )


def test_prometheus_exporter_renders_bounded_latest_snapshot() -> None:
    exporter = PrometheusExporter(max_tasks_per_dag=1)

    exporter.record_success(
        _analysis('demo"dag'),
        duration_seconds=0.25,
        observed_at=1000.0,
    )

    output = exporter.render()
    assert 'flowsense_analysis_success{dag_id="demo\\"dag"} 1' in output
    assert 'flowsense_dag_overall_severity{dag_id="demo\\"dag"} 2' in output
    assert 'flowsense_dag_analysis_coverage_ratio{dag_id="demo\\"dag"} 1' in output
    assert 'flowsense_task_severity{dag_id="demo\\"dag",task_id="extract"} 0' in output
    assert 'task_id="transform"' not in output
    assert 'flowsense_dag_dropped_tasks{dag_id="demo\\"dag"} 1' in output


def test_prometheus_exporter_preserves_results_after_collection_failure() -> None:
    exporter = PrometheusExporter()
    exporter.record_success(_analysis(), duration_seconds=0.2, observed_at=1000.0)
    exporter.record_failure("demo", duration_seconds=0.1, observed_at=1010.0)

    output = exporter.render()
    assert 'flowsense_analysis_success{dag_id="demo"} 0' in output
    assert (
        'flowsense_analysis_last_failure_timestamp_seconds{dag_id="demo"} 1010'
        in output
    )
    assert 'flowsense_dag_runs_analyzed{dag_id="demo"} 5' in output


def test_metrics_http_server_exposes_only_metrics_endpoint() -> None:
    exporter = PrometheusExporter()
    exporter.record_success(_analysis(), duration_seconds=0.2, observed_at=1000.0)
    server = create_metrics_server(exporter, port=0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()

    try:
        with urlopen(
            f"http://127.0.0.1:{server.server_port}/metrics",
            timeout=1,
        ) as response:
            assert response.headers["Content-Type"] == CONTENT_TYPE
            assert b"flowsense_dag_overall_severity" in response.read()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class _RecordingSink:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[str] = []

    def record_success(
        self,
        analysis: DAGAnalysis,
        *,
        duration_seconds: float,
        observed_at: float | None = None,
    ) -> None:
        self.successes.append(analysis.dag_id)

    def record_failure(
        self,
        dag_id: str,
        *,
        duration_seconds: float,
        observed_at: float | None = None,
    ) -> None:
        self.failures.append(dag_id)


def test_collection_isolates_expected_failures_per_dag() -> None:
    sink = _RecordingSink()

    def analyze(request: AnalysisRequest) -> DAGAnalysis:
        if request.dag_id == "broken":
            raise ConfigurationError("broken")
        return _analysis(request.dag_id)

    failed = collect_metrics_once(
        ["healthy", "broken"],
        analyze=analyze,
        sink=sink,
        monotonic=iter([1.0, 1.2, 2.0, 2.3]).__next__,
        clock=lambda: 1000.0,
    )

    assert failed == ["broken"]
    assert sink.successes == ["healthy"]
    assert sink.failures == ["broken"]


@pytest.mark.parametrize("limit", [-1, -100])
def test_prometheus_exporter_rejects_negative_cardinality_limit(limit: int) -> None:
    with pytest.raises(ValueError, match="max_tasks_per_dag"):
        PrometheusExporter(max_tasks_per_dag=limit)
