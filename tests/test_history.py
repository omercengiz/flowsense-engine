from datetime import UTC, datetime

from flowsense.engine.history import build_duration_history
from flowsense.models import TaskRun


def test_build_duration_history() -> None:
    task_runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            state="success",
            start_date=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
            end_date=datetime(2026, 1, 1, 10, 0, 2, tzinfo=UTC),
            duration=2.0,
            try_number=1,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_2",
            task_id="extract",
            state="success",
            start_date=datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
            end_date=datetime(2026, 1, 1, 11, 0, 3, tzinfo=UTC),
            duration=3.0,
            try_number=1,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="transform",
            state="success",
            start_date=datetime(2026, 1, 1, 10, 0, 2, tzinfo=UTC),
            end_date=datetime(2026, 1, 1, 10, 0, 6, tzinfo=UTC),
            duration=4.0,
            try_number=1,
        ),
    ]

    history = build_duration_history(task_runs)

    assert history == {
        "extract": [2.0, 3.0],
        "transform": [4.0],
    }


def test_build_duration_history_ignores_failed_tasks() -> None:
    task_runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            state="failed",
            duration=10.0,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_2",
            task_id="extract",
            state="success",
            duration=2.5,
        ),
    ]

    history = build_duration_history(task_runs)

    assert history == {
        "extract": [2.5],
    }


def test_build_duration_history_ignores_missing_duration() -> None:
    task_runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            state="success",
            duration=None,
        ),
    ]

    history = build_duration_history(task_runs)

    assert history == {}


def test_build_duration_history_uses_slowest_mapped_instance_per_run() -> None:
    task_runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="transform",
            state="success",
            duration=2.0,
            map_index=0,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="transform",
            state="success",
            duration=5.0,
            map_index=1,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_2",
            task_id="transform",
            state="success",
            duration=4.0,
            map_index=0,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_2",
            task_id="transform",
            state="success",
            duration=3.0,
            map_index=1,
        ),
    ]

    history = build_duration_history(task_runs)

    assert history == {"transform": [5.0, 4.0]}
