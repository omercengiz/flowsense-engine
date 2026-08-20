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
    assert analysis.primary_origin == "transform"
    assert len(analysis.propagation_results) == 1
    assert analysis.propagation_results[0].origin_task == "transform"
    assert analysis.propagation_results[0].affected_tasks == ["load"]
