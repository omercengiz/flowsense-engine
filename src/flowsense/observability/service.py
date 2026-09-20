from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterable

from flowsense.application import (
    AnalysisMetricsSink,
    AnalysisRequest,
    AnalyzeDAG,
    DAGDataSourceFactory,
)
from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisPolicy,
    DAGAnalysis,
    FlowSenseError,
)
from flowsense.observability.prometheus import (
    PrometheusExporter,
    create_metrics_server,
)

LOGGER = logging.getLogger(__name__)


def collect_metrics_once(
    dag_ids: Iterable[str],
    *,
    analyze: Callable[[AnalysisRequest], DAGAnalysis],
    sink: AnalysisMetricsSink,
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
    history_run_limit: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    clock: Callable[[], float] = time.time,
) -> list[str]:
    """Analyze every configured DAG once and record independent outcomes."""
    failed: list[str] = []
    for dag_id in dag_ids:
        started = monotonic()
        try:
            analysis = analyze(
                AnalysisRequest(
                    dag_id=dag_id,
                    policy=policy,
                    history_run_limit=history_run_limit,
                )
            )
        except FlowSenseError:
            sink.record_failure(
                dag_id,
                duration_seconds=monotonic() - started,
                observed_at=clock(),
            )
            failed.append(dag_id)
            LOGGER.exception("FlowSense analysis failed for DAG %s", dag_id)
        else:
            sink.record_success(
                analysis,
                duration_seconds=monotonic() - started,
                observed_at=clock(),
            )
    return failed


def run_metrics_service(
    dag_ids: list[str],
    *,
    source_factory: DAGDataSourceFactory,
    host: str,
    port: int,
    interval_seconds: float,
    max_tasks_per_dag: int,
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
    history_run_limit: int | None = None,
) -> None:
    """Continuously analyze DAGs and expose the latest snapshot over HTTP."""
    if not dag_ids:
        raise ValueError("at least one DAG id is required")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    for dag_id in dag_ids:
        AnalysisRequest(
            dag_id=dag_id,
            policy=policy,
            history_run_limit=history_run_limit,
        )

    exporter = PrometheusExporter(
        max_tasks_per_dag=max_tasks_per_dag,
    )
    server = create_metrics_server(exporter, host=host, port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    analyze = AnalyzeDAG(source_factory).execute

    try:
        while True:
            collect_metrics_once(
                dag_ids,
                analyze=analyze,
                sink=exporter,
                policy=policy,
                history_run_limit=history_run_limit,
            )
            time.sleep(interval_seconds)
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join()
