from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from flowsense.domain import DAGAnalysis
from flowsense.domain.enums import SEVERITY_SCORE

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


@dataclass(frozen=True)
class _Metric:
    help: str
    type: str
    samples: dict[tuple[tuple[str, str], ...], float]


class PrometheusExporter:
    """Maintain a bounded Prometheus snapshot of the latest DAG analyses."""

    def __init__(self, *, max_tasks_per_dag: int = 200) -> None:
        if max_tasks_per_dag < 0:
            raise ValueError("max_tasks_per_dag must be non-negative")
        self.max_tasks_per_dag = max_tasks_per_dag
        self._analyses: dict[str, DAGAnalysis] = {}
        self._runtime: dict[str, dict[str, float]] = {}
        self._lock = threading.Lock()

    def record_success(
        self,
        analysis: DAGAnalysis,
        *,
        duration_seconds: float,
        observed_at: float | None = None,
    ) -> None:
        _validate_runtime_value(duration_seconds, "duration_seconds")
        timestamp = time.time() if observed_at is None else observed_at
        _validate_runtime_value(timestamp, "observed_at")
        with self._lock:
            self._analyses[analysis.dag_id] = analysis
            runtime = self._runtime.setdefault(analysis.dag_id, {})
            runtime.update(
                success=1.0,
                duration_seconds=duration_seconds,
                last_success_timestamp_seconds=timestamp,
            )

    def record_failure(
        self,
        dag_id: str,
        *,
        duration_seconds: float,
        observed_at: float | None = None,
    ) -> None:
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        _validate_runtime_value(duration_seconds, "duration_seconds")
        timestamp = time.time() if observed_at is None else observed_at
        _validate_runtime_value(timestamp, "observed_at")
        with self._lock:
            runtime = self._runtime.setdefault(dag_id, {})
            runtime.update(
                success=0.0,
                duration_seconds=duration_seconds,
                last_failure_timestamp_seconds=timestamp,
            )

    def render(self) -> str:
        with self._lock:
            analyses = dict(self._analyses)
            runtime = {dag_id: dict(values) for dag_id, values in self._runtime.items()}

        metrics = _build_metrics(
            analyses,
            runtime,
            max_tasks_per_dag=self.max_tasks_per_dag,
        )
        lines: list[str] = []
        for name in sorted(metrics):
            metric = metrics[name]
            lines.extend(
                (f"# HELP {name} {metric.help}", f"# TYPE {name} {metric.type}")
            )
            for labels, value in sorted(metric.samples.items()):
                suffix = _format_labels(labels)
                lines.append(f"{name}{suffix} {_format_value(value)}")
        return "\n".join(lines) + "\n"


def create_metrics_server(
    exporter: PrometheusExporter,
    *,
    host: str = "127.0.0.1",
    port: int = 9108,
) -> ThreadingHTTPServer:
    """Create an HTTP server exposing the current snapshot at `/metrics`."""
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/metrics":
                self.send_error(404)
                return
            body = exporter.render().encode()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return ThreadingHTTPServer((host, port), MetricsHandler)


def _build_metrics(
    analyses: dict[str, DAGAnalysis],
    runtime: dict[str, dict[str, float]],
    *,
    max_tasks_per_dag: int,
) -> dict[str, _Metric]:
    metrics = _metric_definitions()
    for dag_id, values in runtime.items():
        labels = (("dag_id", dag_id),)
        _set(metrics, "flowsense_analysis_success", labels, values.get("success", 0.0))
        _set(
            metrics,
            "flowsense_analysis_duration_seconds",
            labels,
            values.get("duration_seconds", 0.0),
        )
        for field in (
            "last_success_timestamp_seconds",
            "last_failure_timestamp_seconds",
        ):
            if field in values:
                _set(metrics, f"flowsense_analysis_{field}", labels, values[field])

    for dag_id, analysis in analyses.items():
        labels = (("dag_id", dag_id),)
        summary = analysis.summary
        dag_values = {
            "flowsense_dag_overall_severity": SEVERITY_SCORE[analysis.overall_severity],
            "flowsense_dag_runs_analyzed": analysis.runs_analyzed,
            "flowsense_dag_analysis_coverage_ratio": summary.analysis_coverage_percent
            / 100,
            "flowsense_dag_anomalous_tasks": summary.anomalous_tasks,
            "flowsense_dag_anomalous_handoffs": summary.anomalous_handoffs,
            "flowsense_dag_affected_tasks": summary.affected_tasks,
            "flowsense_dag_change_points": summary.change_points,
            "flowsense_dag_trends": summary.trends,
            "flowsense_dag_diagnostics": summary.diagnostics,
        }
        for name, value in dag_values.items():
            _set(metrics, name, labels, float(value))

        task_ids = sorted(analysis.drift_results)
        selected = task_ids[:max_tasks_per_dag]
        _set(
            metrics,
            "flowsense_dag_dropped_tasks",
            labels,
            float(len(task_ids) - len(selected)),
        )
        for task_id in selected:
            result = analysis.drift_results[task_id]
            task_labels = (("dag_id", dag_id), ("task_id", task_id))
            _set(
                metrics,
                "flowsense_task_severity",
                task_labels,
                float(SEVERITY_SCORE[result.severity]),
            )
            _set(
                metrics,
                "flowsense_task_deviation_percent",
                task_labels,
                result.deviation_percent,
            )
            _set(
                metrics,
                "flowsense_task_robust_z_score",
                task_labels,
                result.robust_z_score,
            )
    return metrics


def _metric_definitions() -> dict[str, _Metric]:
    definitions = {
        "flowsense_analysis_success": "Whether the latest analysis completed successfully.",
        "flowsense_analysis_duration_seconds": "Duration of the latest analysis.",
        "flowsense_analysis_last_success_timestamp_seconds": "Unix time of the latest successful analysis.",
        "flowsense_analysis_last_failure_timestamp_seconds": "Unix time of the latest failed analysis.",
        "flowsense_dag_overall_severity": "Latest DAG severity from 0 (normal) to 3 (critical).",
        "flowsense_dag_runs_analyzed": "Number of DAG runs included in the latest analysis.",
        "flowsense_dag_analysis_coverage_ratio": "Fraction of DAG tasks covered by the latest analysis.",
        "flowsense_dag_anomalous_tasks": "Number of anomalous tasks in the latest analysis.",
        "flowsense_dag_anomalous_handoffs": "Number of anomalous handoffs in the latest analysis.",
        "flowsense_dag_affected_tasks": "Number of affected tasks in the latest analysis.",
        "flowsense_dag_change_points": "Number of detected change points in the latest analysis.",
        "flowsense_dag_trends": "Number of detected trends in the latest analysis.",
        "flowsense_dag_diagnostics": "Number of diagnostics in the latest analysis.",
        "flowsense_dag_dropped_tasks": "Tasks omitted from metrics by the configured cardinality limit.",
        "flowsense_task_severity": "Latest task severity from 0 (normal) to 3 (critical).",
        "flowsense_task_deviation_percent": "Latest task duration deviation percentage.",
        "flowsense_task_robust_z_score": "Latest task robust z-score.",
    }
    return {
        name: _Metric(help=help_text, type="gauge", samples={})
        for name, help_text in definitions.items()
    }


def _set(
    metrics: dict[str, _Metric],
    name: str,
    labels: tuple[tuple[str, str], ...],
    value: float,
) -> None:
    if math.isfinite(value):
        metrics[name].samples[labels] = value


def _format_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    rendered = ",".join(f'{name}="{_escape_label(value)}"' for name, value in labels)
    return "{" + rendered + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_value(value: float) -> str:
    return str(int(value)) if value.is_integer() else repr(value)


def _validate_runtime_value(value: float, name: str) -> None:
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")
