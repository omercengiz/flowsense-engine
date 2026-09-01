from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisPolicy,
    DriftResult,
    InvalidTaskTimingError,
    TaskRun,
)
from flowsense.engine.drift import calculate_drift


@dataclass
class HandoffTiming:
    upstream_task: str
    downstream_task: str
    dag_run_id: str
    handoff_delay: float


@dataclass(frozen=True)
class HandoffHistoryDiagnostic:
    code: str
    upstream_task: str
    downstream_task: str
    dag_run_id: str
    message: str


@dataclass
class HandoffHistoryResult:
    history: dict[tuple[str, str], list[float]]
    diagnostics: list[HandoffHistoryDiagnostic]


def _required_end_date(task_run: TaskRun) -> datetime:
    if task_run.end_date is None:
        raise InvalidTaskTimingError("Upstream task end_date is required.")
    return task_run.end_date


def _required_start_date(task_run: TaskRun) -> datetime:
    if task_run.start_date is None:
        raise InvalidTaskTimingError("Downstream task start_date is required.")
    return task_run.start_date


def calculate_handoff_delay(
    upstream_run: TaskRun,
    downstream_run: TaskRun,
) -> HandoffTiming:
    if upstream_run.end_date is None:
        raise InvalidTaskTimingError("Upstream task end_date is required.")

    if downstream_run.start_date is None:
        raise InvalidTaskTimingError("Downstream task start_date is required.")

    if upstream_run.dag_run_id != downstream_run.dag_run_id:
        raise InvalidTaskTimingError("Task runs must belong to the same DAG run.")

    handoff_delay = (downstream_run.start_date - upstream_run.end_date).total_seconds()

    return HandoffTiming(
        upstream_task=upstream_run.task_id,
        downstream_task=downstream_run.task_id,
        dag_run_id=upstream_run.dag_run_id,
        handoff_delay=handoff_delay,
    )


def build_handoff_history(
    task_runs: list[TaskRun],
    dependencies: dict[str, list[str]],
) -> dict[tuple[str, str], list[float]]:
    return build_handoff_history_with_diagnostics(
        task_runs=task_runs,
        dependencies=dependencies,
    ).history


def build_handoff_history_with_diagnostics(
    task_runs: list[TaskRun],
    dependencies: dict[str, list[str]],
) -> HandoffHistoryResult:
    """Build logical-edge history across regular and dynamically mapped tasks.

    A mapped upstream is complete at its latest instance end, while a mapped
    downstream starts at its earliest instance start.
    """
    runs_by_id: dict[str, dict[str, list[TaskRun]]] = {}

    for task_run in task_runs:
        tasks_in_run = runs_by_id.setdefault(task_run.dag_run_id, {})
        tasks_in_run.setdefault(task_run.task_id, []).append(task_run)

    history: dict[tuple[str, str], list[float]] = {}
    diagnostics: list[HandoffHistoryDiagnostic] = []

    for dag_run_id, tasks_in_run in runs_by_id.items():
        for upstream_task, downstream_tasks in dependencies.items():
            upstream_runs = tasks_in_run.get(upstream_task, [])

            if not upstream_runs:
                for downstream_task in downstream_tasks:
                    diagnostics.append(
                        HandoffHistoryDiagnostic(
                            code="MISSING_UPSTREAM_TASK_RUN",
                            upstream_task=upstream_task,
                            downstream_task=downstream_task,
                            dag_run_id=dag_run_id,
                            message=(
                                f"{upstream_task} has no task run in {dag_run_id}."
                            ),
                        )
                    )
                continue

            if any(task_run.end_date is None for task_run in upstream_runs):
                for downstream_task in downstream_tasks:
                    diagnostics.append(
                        HandoffHistoryDiagnostic(
                            code="MISSING_UPSTREAM_END_DATE",
                            upstream_task=upstream_task,
                            downstream_task=downstream_task,
                            dag_run_id=dag_run_id,
                            message=(
                                f"{upstream_task} has no complete end_date in "
                                f"{dag_run_id}."
                            ),
                        )
                    )
                continue

            upstream_run = max(
                upstream_runs,
                key=_required_end_date,
            )

            for downstream_task in downstream_tasks:
                downstream_runs = tasks_in_run.get(downstream_task, [])

                if not downstream_runs:
                    diagnostics.append(
                        HandoffHistoryDiagnostic(
                            code="MISSING_DOWNSTREAM_TASK_RUN",
                            upstream_task=upstream_task,
                            downstream_task=downstream_task,
                            dag_run_id=dag_run_id,
                            message=(
                                f"{downstream_task} has no task run in {dag_run_id}."
                            ),
                        )
                    )
                    continue

                if any(task_run.start_date is None for task_run in downstream_runs):
                    diagnostics.append(
                        HandoffHistoryDiagnostic(
                            code="MISSING_DOWNSTREAM_START_DATE",
                            upstream_task=upstream_task,
                            downstream_task=downstream_task,
                            dag_run_id=dag_run_id,
                            message=(
                                f"{downstream_task} has no complete start_date in "
                                f"{dag_run_id}."
                            ),
                        )
                    )
                    continue

                downstream_run = min(
                    downstream_runs,
                    key=_required_start_date,
                )

                try:
                    timing = calculate_handoff_delay(
                        upstream_run=upstream_run,
                        downstream_run=downstream_run,
                    )
                except InvalidTaskTimingError as exc:
                    diagnostics.append(
                        HandoffHistoryDiagnostic(
                            code="INVALID_HANDOFF_TIMING",
                            upstream_task=upstream_task,
                            downstream_task=downstream_task,
                            dag_run_id=dag_run_id,
                            message=str(exc),
                        )
                    )
                    continue

                edge = (
                    upstream_task,
                    downstream_task,
                )

                history.setdefault(edge, []).append(timing.handoff_delay)

    return HandoffHistoryResult(
        history=history,
        diagnostics=diagnostics,
    )


def calculate_handoff_drift(
    upstream_task: str,
    downstream_task: str,
    handoff_delays: list[float],
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
) -> DriftResult:
    edge_id = f"{upstream_task}->{downstream_task}"

    return calculate_drift(
        task_id=edge_id,
        durations=handoff_delays,
        policy=policy,
    )
