from __future__ import annotations

from flowsense.collector.airflow_client import AirflowClient
from flowsense.engine.drift import calculate_drift
from flowsense.engine.history import build_duration_history
from flowsense.engine.impact import classify_task_impact
from flowsense.engine.propagation import analyze_propagation
from flowsense.engine.root_cause import select_primary_origin
from flowsense.engine.timing import (
    build_handoff_history,
    calculate_handoff_drift,
)
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

    handoff_history = build_handoff_history(
        task_runs=task_runs,
        dependencies=dependencies,
    )

    handoff_drift_results = {}

    for edge, delays in handoff_history.items():
        _upstream_task, downstream_task = edge

        try:
            handoff_drift_results[edge] = calculate_handoff_drift(
                upstream_task=_upstream_task,
                downstream_task=downstream_task,
                handoff_delays=delays,
            )
        except ValueError:
            continue

    task_impacts = {}

    for task_id, task_drift in drift_results.items():
        upstream_handoff_drifts = [
            drift
            for (
                _upstream_task,
                downstream_task,
            ), drift in handoff_drift_results.items()
            if downstream_task == task_id
        ]

        task_impacts[task_id] = classify_task_impact(
            task_id=task_id,
            task_drift=task_drift,
            upstream_handoff_drifts=upstream_handoff_drifts,
        )

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

    all_drift_results = [
        *drift_results.values(),
        *handoff_drift_results.values(),
    ]

    if all_drift_results:
        overall_severity = max(
            all_drift_results,
            key=lambda result: severity_order[result.severity],
        ).severity

    primary_origin = None

    if propagation_results:
        primary_origin = select_primary_origin(
            drift_results=drift_results,
            task_impacts=task_impacts,
            dependencies=dependencies,
            propagation_results=propagation_results,
        )

    return DAGAnalysis(
        dag_id=dag_id,
        runs_analyzed=len({run.dag_run_id for run in task_runs}),
        overall_severity=overall_severity,
        primary_origin=primary_origin,
        drift_results=drift_results,
        handoff_drift_results=handoff_drift_results,
        task_impacts=task_impacts,
        propagation_results=propagation_results,
        dependencies=dependencies,
    )
