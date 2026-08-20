from __future__ import annotations

from flowsense.collector.airflow_client import AirflowClient
from flowsense.engine.drift import calculate_drift
from flowsense.engine.history import build_duration_history
from flowsense.engine.propagation import analyze_propagation
from flowsense.models import DAGAnalysis


def analyze_dag(dag_id: str) -> DAGAnalysis:
    client = AirflowClient()

    task_runs = client.collect_task_runs(dag_id)
    duration_history = build_duration_history(task_runs)

    drift_results = {}

    for task_id, durations in duration_history.items():
        try:
            drift_results[task_id] = calculate_drift(
                task_id=task_id,
                durations=durations,
            )
        except ValueError:
            continue

    dependencies = client.get_dag_dependencies(dag_id)

    propagation_results = analyze_propagation(
        drift_results=drift_results,
        dependencies=dependencies,
    )

    severity_order = {
        "NORMAL": 0,
        "MEDIUM": 1,
        "HIGH": 2,
        "CRITICAL": 3,
    }

    overall_severity = "NORMAL"

    if drift_results:
        overall_severity = max(
            drift_results.values(),
            key=lambda result: severity_order[result.severity],
        ).severity

    primary_origin = None

    if propagation_results:
        primary_origin = max(
            propagation_results,
            key=lambda result: result.propagation_score,
        ).origin_task

    return DAGAnalysis(
        dag_id=dag_id,
        runs_analyzed=len({run.dag_run_id for run in task_runs}),
        overall_severity=overall_severity,
        primary_origin=primary_origin,
        drift_results=drift_results,
        propagation_results=propagation_results,
        dependencies=dependencies,
    )
