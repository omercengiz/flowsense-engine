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
    """Build logical-edge history across regular and dynamically mapped tasks.

    A mapped upstream is complete at its latest instance end, while a mapped
    downstream starts at its earliest instance start.
    """
    runs_by_id: dict[str, dict[str, list[TaskRun]]] = {}

    for task_run in task_runs:
        tasks_in_run = runs_by_id.setdefault(task_run.dag_run_id, {})
        tasks_in_run.setdefault(task_run.task_id, []).append(task_run)

    history: dict[tuple[str, str], list[float]] = {}

    for tasks_in_run in runs_by_id.values():
        for upstream_task, downstream_tasks in dependencies.items():
            upstream_runs = tasks_in_run.get(upstream_task, [])
            upstream_runs_with_end = [
                task_run for task_run in upstream_runs if task_run.end_date is not None
            ]

            if not upstream_runs_with_end:
                continue

            upstream_run = max(
                upstream_runs_with_end,
                key=lambda task_run: task_run.end_date,
            )

            for downstream_task in downstream_tasks:
                downstream_runs = tasks_in_run.get(downstream_task, [])
                downstream_runs_with_start = [
                    task_run
                    for task_run in downstream_runs
                    if task_run.start_date is not None
                ]

                if not downstream_runs_with_start:
                    continue

                downstream_run = min(
                    downstream_runs_with_start,
                    key=lambda task_run: task_run.start_date,
                )

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
