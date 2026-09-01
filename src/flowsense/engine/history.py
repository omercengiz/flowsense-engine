from __future__ import annotations

from collections import defaultdict

from flowsense.domain import TaskRun


def build_duration_history(
    task_runs: list[TaskRun],
) -> dict[str, list[float]]:
    """Build logical-task history using the slowest mapped instance per DAG run."""
    durations_by_run_and_task: dict[tuple[str, str], float] = {}

    for task_run in task_runs:
        if task_run.state != "success" or task_run.duration is None:
            continue

        key = (task_run.dag_run_id, task_run.task_id)
        current_duration = durations_by_run_and_task.get(key)

        if current_duration is None or task_run.duration > current_duration:
            durations_by_run_and_task[key] = task_run.duration

    history: dict[str, list[float]] = defaultdict(list)

    for (_dag_run_id, task_id), duration in durations_by_run_and_task.items():
        history[task_id].append(duration)

    return dict(history)
