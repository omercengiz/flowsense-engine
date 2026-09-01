from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from flowsense.application import DAGDataSource, analyze_dag
from flowsense.models import TaskRun


def test_analyze_dag_identifies_primary_origin() -> None:
    client = MagicMock(spec=DAGDataSource)

    client.collect_task_runs.return_value = [
        TaskRun(
            dag_id="demo",
            dag_run_id=f"run_{index}",
            task_id="transform",
            state="success",
            duration=duration,
        )
        for index, duration in enumerate(
            [3.0, 3.1, 2.9, 3.0, 9.5],
            start=1,
        )
    ] + [
        TaskRun(
            dag_id="demo",
            dag_run_id=f"run_{index}",
            task_id="load",
            state="success",
            duration=duration,
        )
        for index, duration in enumerate(
            [1.0, 1.1, 0.9, 1.0, 2.0],
            start=1,
        )
    ]

    client.get_dag_dependencies.return_value = {
        "transform": ["load"],
        "load": [],
    }

    analysis = analyze_dag("demo", client)

    assert analysis.overall_severity == "CRITICAL"
    assert analysis.primary_origin is not None
    assert analysis.primary_origin.task_id == "transform"
    assert len(analysis.propagation_results) == 1
    assert analysis.propagation_results[0].origin_task == "transform"
    assert analysis.propagation_results[0].affected_tasks == ["load"]


def test_analyze_dag_identifies_isolated_primary_origin() -> None:
    client = MagicMock(spec=DAGDataSource)

    client.collect_task_runs.return_value = [
        TaskRun(
            dag_id="demo",
            dag_run_id=f"run_{index}",
            task_id="transform",
            state="success",
            duration=duration,
        )
        for index, duration in enumerate(
            [3.0, 3.1, 2.9, 3.0, 9.5],
            start=1,
        )
    ]

    client.get_dag_dependencies.return_value = {
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    assert analysis.propagation_results == []
    assert analysis.primary_origin is not None
    assert analysis.primary_origin.task_id == "transform"


def test_analyze_dag_detects_task_and_handoff_change_points() -> None:
    client = MagicMock(spec=DAGDataSource)
    base_time = datetime(2026, 8, 20, 10, 0, tzinfo=UTC)
    task_runs = []

    for index, (duration, delay) in enumerate(
        zip(
            [3.0, 3.1, 2.9, 6.0, 6.1, 5.9],
            [1.0, 1.1, 0.9, 4.0, 4.1, 3.9],
            strict=True,
        ),
        start=1,
    ):
        extract_end = base_time + timedelta(minutes=index)
        transform_start = extract_end + timedelta(seconds=delay)
        task_runs.extend(
            [
                TaskRun(
                    dag_id="demo",
                    dag_run_id=f"run_{index}",
                    task_id="extract",
                    state="success",
                    end_date=extract_end,
                    duration=1.0,
                ),
                TaskRun(
                    dag_id="demo",
                    dag_run_id=f"run_{index}",
                    task_id="transform",
                    state="success",
                    start_date=transform_start,
                    duration=duration,
                ),
            ]
        )

    client.collect_task_runs.return_value = task_runs
    client.get_dag_dependencies.return_value = {
        "extract": ["transform"],
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    task_change = analysis.change_point_results["transform"]
    handoff_change = analysis.handoff_change_point_results[("extract", "transform")]

    assert task_change.change_index == 3
    assert task_change.direction == "INCREASE"
    assert handoff_change.change_index == 3
    assert handoff_change.direction == "INCREASE"


def test_analyze_dag_detects_task_and_handoff_trends() -> None:
    client = MagicMock(spec=DAGDataSource)
    base_time = datetime(2026, 8, 20, 10, 0, tzinfo=UTC)
    task_runs = []

    for index, (duration, delay) in enumerate(
        zip(
            [3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            strict=True,
        ),
        start=1,
    ):
        extract_end = base_time + timedelta(minutes=index)
        transform_start = extract_end + timedelta(seconds=delay)
        task_runs.extend(
            [
                TaskRun(
                    dag_id="demo",
                    dag_run_id=f"run_{index}",
                    task_id="extract",
                    state="success",
                    end_date=extract_end,
                    duration=1.0,
                ),
                TaskRun(
                    dag_id="demo",
                    dag_run_id=f"run_{index}",
                    task_id="transform",
                    state="success",
                    start_date=transform_start,
                    duration=duration,
                ),
            ]
        )

    client.collect_task_runs.return_value = task_runs
    client.get_dag_dependencies.return_value = {
        "extract": ["transform"],
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    task_trend = analysis.trend_results["transform"]
    handoff_trend = analysis.handoff_trend_results[("extract", "transform")]

    assert task_trend.direction == "INCREASING"
    assert task_trend.slope_per_observation == 1.0
    assert handoff_trend.direction == "INCREASING"
    assert handoff_trend.slope_per_observation == 1.0


def test_analyze_dag_calculates_handoff_drift() -> None:
    client = MagicMock(spec=DAGDataSource)

    base_time = datetime(
        2026,
        8,
        20,
        10,
        0,
        tzinfo=UTC,
    )

    handoff_delays = [
        2.0,
        2.1,
        1.9,
        2.0,
        8.0,
    ]

    task_runs = []

    for index, delay in enumerate(
        handoff_delays,
        start=1,
    ):
        dag_run_id = f"run_{index}"

        extract_start = base_time + timedelta(minutes=index)
        extract_end = extract_start + timedelta(seconds=1)

        transform_start = extract_end + timedelta(seconds=delay)
        transform_end = transform_start + timedelta(seconds=3)

        task_runs.extend(
            [
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="extract",
                    state="success",
                    start_date=extract_start,
                    end_date=extract_end,
                    duration=1.0,
                ),
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="transform",
                    state="success",
                    start_date=transform_start,
                    end_date=transform_end,
                    duration=3.0,
                ),
            ]
        )

    client.collect_task_runs.return_value = task_runs

    client.get_dag_dependencies.return_value = {
        "extract": ["transform"],
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    edge = ("extract", "transform")

    assert edge in analysis.handoff_drift_results

    handoff_drift = analysis.handoff_drift_results[edge]

    assert handoff_drift.current == 8.0
    assert handoff_drift.baseline == 2.0
    assert handoff_drift.severity == "CRITICAL"


def test_analyze_dag_uses_handoff_for_overall_severity() -> None:
    client = MagicMock(spec=DAGDataSource)

    base_time = datetime(
        2026,
        8,
        20,
        10,
        0,
        tzinfo=UTC,
    )

    handoff_delays = [
        2.0,
        2.1,
        1.9,
        2.0,
        8.0,
    ]

    task_runs = []

    for index, delay in enumerate(
        handoff_delays,
        start=1,
    ):
        dag_run_id = f"run_{index}"

        extract_start = base_time + timedelta(minutes=index)
        extract_end = extract_start + timedelta(seconds=1)

        transform_start = extract_end + timedelta(seconds=delay)
        transform_end = transform_start + timedelta(seconds=3)

        task_runs.extend(
            [
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="extract",
                    state="success",
                    start_date=extract_start,
                    end_date=extract_end,
                    duration=1.0,
                ),
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="transform",
                    state="success",
                    start_date=transform_start,
                    end_date=transform_end,
                    duration=3.0,
                ),
            ]
        )

    client.collect_task_runs.return_value = task_runs

    client.get_dag_dependencies.return_value = {
        "extract": ["transform"],
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    assert analysis.drift_results["extract"].severity == "NORMAL"
    assert analysis.drift_results["transform"].severity == "NORMAL"

    assert (
        analysis.handoff_drift_results[("extract", "transform")].severity == "CRITICAL"
    )

    assert analysis.overall_severity == "CRITICAL"


def test_analyze_dag_reports_insufficient_history_diagnostics() -> None:
    client = MagicMock(spec=DAGDataSource)
    base_time = datetime(2026, 8, 20, 10, 0, tzinfo=UTC)
    task_runs = []

    for index in range(4):
        dag_run_id = f"run_{index}"
        extract_end = base_time + timedelta(minutes=index)

        task_runs.extend(
            [
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="extract",
                    state="success",
                    end_date=extract_end,
                    duration=1.0,
                ),
                TaskRun(
                    dag_id="demo",
                    dag_run_id=dag_run_id,
                    task_id="transform",
                    state="success",
                    start_date=extract_end + timedelta(seconds=2),
                    duration=3.0,
                ),
            ]
        )

    client.collect_task_runs.return_value = task_runs
    client.get_dag_dependencies.return_value = {
        "extract": ["transform"],
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    diagnostics = {
        (diagnostic.code, diagnostic.subject_id) for diagnostic in analysis.diagnostics
    }

    assert diagnostics == {
        ("INSUFFICIENT_TASK_HISTORY", "extract"),
        ("INSUFFICIENT_TASK_HISTORY", "transform"),
        ("INSUFFICIENT_HANDOFF_HISTORY", "extract->transform"),
    }


def test_analyze_dag_reports_missing_handoff_timestamp() -> None:
    client = MagicMock(spec=DAGDataSource)
    client.collect_task_runs.return_value = [
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
    ]
    client.get_dag_dependencies.return_value = {
        "extract": ["transform"],
        "transform": [],
    }

    analysis = analyze_dag("demo", client)

    assert any(
        diagnostic.code == "MISSING_UPSTREAM_END_DATE"
        and diagnostic.subject_id == "extract->transform"
        for diagnostic in analysis.diagnostics
    )


def test_analyze_dag_does_not_hide_unexpected_value_errors() -> None:
    client = MagicMock(spec=DAGDataSource)
    client.collect_task_runs.return_value = [
        TaskRun(
            dag_id="demo",
            dag_run_id=f"run_{index}",
            task_id="transform",
            state="success",
            duration=3.0,
        )
        for index in range(5)
    ]
    client.get_dag_dependencies.return_value = {"transform": []}

    with (
        patch(
            "flowsense.application.analyzer.calculate_drift",
            side_effect=ValueError("unexpected failure"),
        ),
        pytest.raises(ValueError, match="unexpected failure"),
    ):
        analyze_dag("demo", client)
