from __future__ import annotations

from dataclasses import dataclass

from flowsense.engine.drift import DriftResult, calculate_drift
from flowsense.models import TaskRun


@dataclass
class HandoffTiming:
    upstream_task: str
    downstream_task: str
    dag_run_id: str
    handoff_delay: float


def calculate_handoff_delay(
    upstream_run: TaskRun,
    downstream_run: TaskRun,
) -> HandoffTiming:
    if upstream_run.end_date is None:
        raise ValueError("Upstream task end_date is required.")

    if downstream_run.start_date is None:
        raise ValueError("Downstream task start_date is required.")

    if upstream_run.dag_run_id != downstream_run.dag_run_id:
        raise ValueError("Task runs must belong to the same DAG run.")

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
    runs_by_id: dict[str, dict[str, TaskRun]] = {}

    for task_run in task_runs:
        runs_by_id.setdefault(task_run.dag_run_id, {})[task_run.task_id] = task_run

    history: dict[tuple[str, str], list[float]] = {}

    for tasks_in_run in runs_by_id.values():
        for upstream_task, downstream_tasks in dependencies.items():
            upstream_run = tasks_in_run.get(upstream_task)

            if upstream_run is None:
                continue

            for downstream_task in downstream_tasks:
                downstream_run = tasks_in_run.get(downstream_task)

                if downstream_run is None:
                    continue

                try:
                    timing = calculate_handoff_delay(
                        upstream_run=upstream_run,
                        downstream_run=downstream_run,
                    )
                except ValueError:
                    continue

                edge = (
                    upstream_task,
                    downstream_task,
                )

                history.setdefault(edge, []).append(timing.handoff_delay)

    return history


def calculate_handoff_drift(
    upstream_task: str,
    downstream_task: str,
    handoff_delays: list[float],
) -> DriftResult:
    edge_id = f"{upstream_task}->{downstream_task}"

    return calculate_drift(
        task_id=edge_id,
        durations=handoff_delays,
    )
