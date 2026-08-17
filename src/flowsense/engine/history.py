from __future__ import annotations

from collections import defaultdict

from flowsense.models import TaskRun


def build_duration_history(
    task_runs: list[TaskRun],
) -> dict[str, list[float]]:
    history: dict[str, list[float]] = defaultdict(list)

    for task_run in task_runs:
        if task_run.state == "success" and task_run.duration is not None:
            history[task_run.task_id].append(task_run.duration)

    return dict(history)
