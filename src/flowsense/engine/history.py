from __future__ import annotations

from collections import defaultdict

from flowsense.domain import MappedTaskAggregation, TaskRun


def build_duration_history(
    task_runs: list[TaskRun],
    aggregation: MappedTaskAggregation = MappedTaskAggregation.MAX,
) -> dict[str, list[float]]:
    """Build logical-task history using the slowest mapped instance per DAG run."""
    grouped_durations: dict[tuple[str, str], list[float]] = defaultdict(list)

    for task_run in task_runs:
        if task_run.state != "success" or task_run.duration is None:
            continue

        key = (task_run.dag_run_id, task_run.task_id)
        grouped_durations[key].append(task_run.duration)

    def aggregate(values: list[float]) -> float:
        if aggregation is MappedTaskAggregation.SUM:
            return sum(values)
        if aggregation is MappedTaskAggregation.MEAN:
            return sum(values) / len(values)
        return max(values)

    history: dict[str, list[float]] = defaultdict(list)

    for (_dag_run_id, task_id), durations in grouped_durations.items():
        history[task_id].append(aggregate(durations))

    return dict(history)
