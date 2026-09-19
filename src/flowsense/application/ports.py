from typing import Protocol

from flowsense.domain import AnalysisPolicy, DAGAnalysis, TaskRun


class DAGDataSource(Protocol):
    def collect_task_runs(self, dag_id: str) -> list[TaskRun]:
        """Return task runs ordered from oldest DAG run to newest."""
        ...

    def get_dag_dependencies(self, dag_id: str) -> dict[str, list[str]]: ...


class DAGAnalysisEngine(Protocol):
    """Analyze already collected domain data without performing I/O."""

    def analyze(
        self,
        dag_id: str,
        task_runs: list[TaskRun],
        dependencies: dict[str, list[str]],
        policy: AnalysisPolicy,
    ) -> DAGAnalysis: ...


class AnalysisMetricsSink(Protocol):
    """Receive analysis lifecycle events without coupling use cases to a backend."""

    def record_success(
        self,
        analysis: DAGAnalysis,
        *,
        duration_seconds: float,
        observed_at: float | None = None,
    ) -> None: ...

    def record_failure(
        self,
        dag_id: str,
        *,
        duration_seconds: float,
        observed_at: float | None = None,
    ) -> None: ...
