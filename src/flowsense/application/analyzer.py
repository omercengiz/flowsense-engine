from __future__ import annotations

from flowsense.application.pipeline import (
    analyze_handoff_histories,
    analyze_task_histories,
    classify_task_impacts,
    determine_overall_severity,
)
from flowsense.application.ports import DAGDataSource
from flowsense.domain import DEFAULT_ANALYSIS_POLICY, AnalysisPolicy, DAGAnalysis
from flowsense.engine.history import build_duration_history
from flowsense.engine.propagation import analyze_propagation
from flowsense.engine.root_cause import select_primary_origin


def analyze_dag(
    dag_id: str,
    source: DAGDataSource,
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
) -> DAGAnalysis:
    task_runs = source.collect_task_runs(dag_id)
    dependencies = source.get_dag_dependencies(dag_id)
    duration_history = build_duration_history(
        task_runs,
        aggregation=policy.mapped_task_aggregation,
    )

    task_analysis = analyze_task_histories(duration_history, policy)
    handoff_analysis = analyze_handoff_histories(task_runs, dependencies, policy)
    task_impacts = classify_task_impacts(
        task_analysis.drift_results,
        handoff_analysis.drift_results,
    )
    propagation_results = analyze_propagation(
        drift_results=task_analysis.drift_results,
        dependencies=dependencies,
    )
    primary_origin = select_primary_origin(
        drift_results=task_analysis.drift_results,
        task_impacts=task_impacts,
        dependencies=dependencies,
        propagation_results=propagation_results,
    )

    return DAGAnalysis(
        dag_id=dag_id,
        runs_analyzed=len({run.dag_run_id for run in task_runs}),
        overall_severity=determine_overall_severity(
            task_analysis.drift_results,
            handoff_analysis.drift_results,
        ),
        primary_origin=primary_origin,
        drift_results=task_analysis.drift_results,
        handoff_drift_results=handoff_analysis.drift_results,
        task_impacts=task_impacts,
        propagation_results=propagation_results,
        dependencies=dependencies,
        diagnostics=[
            *task_analysis.diagnostics,
            *handoff_analysis.diagnostics,
        ],
        policy=policy,
        change_point_results=task_analysis.change_point_results,
        handoff_change_point_results=handoff_analysis.change_point_results,
        trend_results=task_analysis.trend_results,
        handoff_trend_results=handoff_analysis.trend_results,
    )
