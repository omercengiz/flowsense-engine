from datetime import UTC, datetime, timedelta

import pytest

from flowsense.engine.timing import (
    build_handoff_history,
    build_handoff_history_with_diagnostics,
    calculate_handoff_delay,
    calculate_handoff_drift,
)
from flowsense.models import TaskRun


def test_calculate_handoff_delay() -> None:
    upstream_end = datetime(2026, 8, 20, 10, 0, 5, tzinfo=UTC)

    upstream_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_1",
        task_id="extract",
        state="success",
        end_date=upstream_end,
    )

    downstream_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_1",
        task_id="transform",
        state="success",
        start_date=upstream_end + timedelta(seconds=7),
    )

    result = calculate_handoff_delay(
        upstream_run=upstream_run,
        downstream_run=downstream_run,
    )

    assert result.upstream_task == "extract"
    assert result.downstream_task == "transform"
    assert result.dag_run_id == "run_1"
    assert result.handoff_delay == 7.0


def test_calculate_handoff_delay_requires_upstream_end_date() -> None:
    upstream_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_1",
        task_id="extract",
    )

    downstream_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_1",
        task_id="transform",
        start_date=datetime(2026, 8, 20, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(ValueError):
        calculate_handoff_delay(
            upstream_run=upstream_run,
            downstream_run=downstream_run,
        )


def test_calculate_handoff_delay_requires_same_dag_run() -> None:
    upstream_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_1",
        task_id="extract",
        end_date=datetime(2026, 8, 20, 10, 0, tzinfo=UTC),
    )

    downstream_run = TaskRun(
        dag_id="demo",
        dag_run_id="run_2",
        task_id="transform",
        start_date=datetime(2026, 8, 20, 10, 0, 5, tzinfo=UTC),
    )

    with pytest.raises(ValueError):
        calculate_handoff_delay(
            upstream_run=upstream_run,
            downstream_run=downstream_run,
        )


def test_build_handoff_history() -> None:
    base_time = datetime(
        2026,
        8,
        20,
        10,
        0,
        tzinfo=UTC,
    )

    task_runs = []

    for index, delay in enumerate(
        [2.0, 3.0, 4.0],
        start=1,
    ):
        dag_run_id = f"run_{index}"

        upstream_end = base_time + timedelta(
            minutes=index,
        )

        task_runs.extend(
            [
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="extract",
                    state="success",
                    end_date=upstream_end,
                ),
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="transform",
                    state="success",
                    start_date=upstream_end + timedelta(seconds=delay),
                ),
            ]
        )

    dependencies = {
        "extract": ["transform"],
        "transform": [],
    }

    history = build_handoff_history(
        task_runs=task_runs,
        dependencies=dependencies,
    )

    assert history == {
        ("extract", "transform"): [
            2.0,
            3.0,
            4.0,
        ]
    }


def test_build_handoff_history_aggregates_mapped_task_boundaries() -> None:
    base_time = datetime(2026, 8, 20, 10, 0, tzinfo=UTC)

    task_runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            state="success",
            end_date=base_time + timedelta(seconds=10),
            map_index=0,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            state="success",
            end_date=base_time + timedelta(seconds=5),
            map_index=1,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="transform",
            state="success",
            start_date=base_time + timedelta(seconds=12),
            map_index=0,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="transform",
            state="success",
            start_date=base_time + timedelta(seconds=20),
            map_index=1,
        ),
    ]

    history = build_handoff_history(
        task_runs=task_runs,
        dependencies={
            "extract": ["transform"],
            "transform": [],
        },
    )

    assert history == {("extract", "transform"): [2.0]}


def test_build_handoff_history_reports_missing_timestamps() -> None:
    task_runs = [
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="extract",
            state="success",
            end_date=None,
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_1",
            task_id="transform",
            state="success",
            start_date=datetime(2026, 8, 20, 10, 0, tzinfo=UTC),
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_2",
            task_id="extract",
            state="success",
            end_date=datetime(2026, 8, 20, 11, 0, tzinfo=UTC),
        ),
        TaskRun(
            dag_id="demo",
            dag_run_id="run_2",
            task_id="transform",
            state="success",
            start_date=None,
        ),
    ]

    result = build_handoff_history_with_diagnostics(
        task_runs=task_runs,
        dependencies={
            "extract": ["transform"],
            "transform": [],
        },
    )

    assert result.history == {}
    assert [diagnostic.code for diagnostic in result.diagnostics] == [
        "MISSING_UPSTREAM_END_DATE",
        "MISSING_DOWNSTREAM_START_DATE",
    ]


def test_calculate_handoff_drift_detects_anomaly() -> None:
    result = calculate_handoff_drift(
        upstream_task="extract",
        downstream_task="transform",
        handoff_delays=[
            2.0,
            2.1,
            1.9,
            2.0,
            8.0,
        ],
    )

    assert result.task_id == "extract->transform"
    assert result.severity == "CRITICAL"
    assert result.current == 8.0
    assert result.baseline == 2.0
