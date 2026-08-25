from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from flowsense.engine.analyzer import analyze_dag
from flowsense.models import TaskRun


@patch("flowsense.engine.analyzer.AirflowClient")
def test_analyze_dag_identifies_primary_origin(
    mock_client_class: MagicMock,
) -> None:
    client = mock_client_class.return_value

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

    analysis = analyze_dag("demo")

    assert analysis.overall_severity == "CRITICAL"
    assert analysis.primary_origin is not None
    assert analysis.primary_origin.task_id == "transform"
    assert len(analysis.propagation_results) == 1
    assert analysis.propagation_results[0].origin_task == "transform"
    assert analysis.propagation_results[0].affected_tasks == ["load"]


@patch("flowsense.engine.analyzer.AirflowClient")
def test_analyze_dag_calculates_handoff_drift(
    mock_client_class: MagicMock,
) -> None:
    client = mock_client_class.return_value

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

    analysis = analyze_dag("demo")

    edge = ("extract", "transform")

    assert edge in analysis.handoff_drift_results

    handoff_drift = analysis.handoff_drift_results[edge]

    assert handoff_drift.current == 8.0
    assert handoff_drift.baseline == 2.0
    assert handoff_drift.severity == "CRITICAL"


@patch("flowsense.engine.analyzer.AirflowClient")
def test_analyze_dag_uses_handoff_for_overall_severity(
    mock_client_class: MagicMock,
) -> None:
    client = mock_client_class.return_value

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

    analysis = analyze_dag("demo")

    assert analysis.drift_results["extract"].severity == "NORMAL"
    assert analysis.drift_results["transform"].severity == "NORMAL"

    assert (
        analysis.handoff_drift_results[("extract", "transform")].severity == "CRITICAL"
    )

    assert analysis.overall_severity == "CRITICAL"
