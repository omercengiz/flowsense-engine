from __future__ import annotations

import pytest

from flowsense.collector.airflow_client import AirflowClient
from flowsense.engine.drift import calculate_drift
from flowsense.engine.history import build_duration_history
from flowsense.engine.propagation import analyze_propagation

DAG_ID = "flowsense_demo"


@pytest.mark.integration
def test_flowsense_demo_analysis() -> None:
    client = AirflowClient()

    task_runs = client.collect_task_runs(DAG_ID)

    assert task_runs

    history = build_duration_history(task_runs)

    assert history
    assert "extract" in history
    assert "transform" in history
    assert "load" in history

    drift_results = {}

    for task_id, durations in history.items():
        result = calculate_drift(
            task_id,
            durations,
        )

        drift_results[task_id] = result

    assert drift_results["extract"].severity in {
        "NORMAL",
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    }

    assert drift_results["transform"].severity in {
        "NORMAL",
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    }

    assert drift_results["load"].severity in {
        "NORMAL",
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    }

    dependencies = client.get_dag_dependencies(DAG_ID)

    assert dependencies["extract"] == ["transform"]
    assert dependencies["transform"] == ["load"]
    assert dependencies["load"] == []

    propagation_results = analyze_propagation(
        drift_results,
        dependencies,
    )

    assert isinstance(
        propagation_results,
        list,
    )
