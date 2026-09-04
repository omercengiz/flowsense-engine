from __future__ import annotations

from flowsense.application.ports import DAGDataSource
from flowsense.domain import (
    DEFAULT_ANALYSIS_POLICY,
    AnalysisDiagnostic,
    AnalysisPolicy,
    DAGAnalysis,
    InsufficientHistoryError,
    Severity,
)
from flowsense.domain.enums import SEVERITY_SCORE
from flowsense.engine.change_point import detect_change_point
from flowsense.engine.drift import calculate_drift
from flowsense.engine.history import build_duration_history
from flowsense.engine.impact import classify_task_impact
from flowsense.engine.propagation import analyze_propagation
from flowsense.engine.root_cause import select_primary_origin
from flowsense.engine.timing import (
    build_handoff_history_with_diagnostics,
    calculate_handoff_drift,
)
from flowsense.engine.trend import detect_trend


def analyze_dag(
    dag_id: str,
    source: DAGDataSource,
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
) -> DAGAnalysis:
    task_runs = source.collect_task_runs(dag_id)
    duration_history = build_duration_history(
        task_runs,
        aggregation=policy.mapped_task_aggregation,
    )
    diagnostics: list[AnalysisDiagnostic] = []
    change_point_results = {}
    trend_results = {}

    drift_results = {}

    for task_id, durations in duration_history.items():
        if policy.change_point_detection_enabled:
            change_point = detect_change_point(
                task_id,
                durations,
                minimum_segment_size=policy.change_point_minimum_segment_size,
                score_threshold=policy.change_point_score_threshold,
            )
            if change_point is not None:
                change_point_results[task_id] = change_point

        if policy.trend_detection_enabled:
            trend = detect_trend(
                task_id,
                durations,
                minimum_observations=policy.trend_minimum_observations,
                score_threshold=policy.trend_score_threshold,
                minimum_directional_consistency=(
                    policy.trend_minimum_directional_consistency
                ),
            )
            if trend is not None:
                trend_results[task_id] = trend

        try:
            drift_results[task_id] = calculate_drift(
                task_id=task_id,
                durations=durations,
                policy=policy,
            )
        except InsufficientHistoryError as exc:
            diagnostics.append(
                AnalysisDiagnostic(
                    code="INSUFFICIENT_TASK_HISTORY",
                    subject_id=task_id,
                    message=str(exc),
                )
            )

    dependencies = source.get_dag_dependencies(dag_id)

    handoff_history_result = build_handoff_history_with_diagnostics(
        task_runs=task_runs,
        dependencies=dependencies,
    )
    handoff_history = handoff_history_result.history
    handoff_change_point_results = {}
    handoff_trend_results = {}

    diagnostics.extend(
        AnalysisDiagnostic(
            code=diagnostic.code,
            subject_id=f"{diagnostic.upstream_task}->{diagnostic.downstream_task}",
            message=diagnostic.message,
        )
        for diagnostic in handoff_history_result.diagnostics
    )

    handoff_drift_results = {}

    for edge, delays in handoff_history.items():
        upstream_task, downstream_task = edge
        subject_id = f"{upstream_task}->{downstream_task}"

        if policy.change_point_detection_enabled:
            change_point = detect_change_point(
                subject_id,
                delays,
                minimum_segment_size=policy.change_point_minimum_segment_size,
                score_threshold=policy.change_point_score_threshold,
            )
            if change_point is not None:
                handoff_change_point_results[edge] = change_point

        if policy.trend_detection_enabled:
            trend = detect_trend(
                subject_id,
                delays,
                minimum_observations=policy.trend_minimum_observations,
                score_threshold=policy.trend_score_threshold,
                minimum_directional_consistency=(
                    policy.trend_minimum_directional_consistency
                ),
            )
            if trend is not None:
                handoff_trend_results[edge] = trend

        try:
            handoff_drift_results[edge] = calculate_handoff_drift(
                upstream_task=upstream_task,
                downstream_task=downstream_task,
                handoff_delays=delays,
                policy=policy,
            )
        except InsufficientHistoryError as exc:
            diagnostics.append(
                AnalysisDiagnostic(
                    code="INSUFFICIENT_HANDOFF_HISTORY",
                    subject_id=f"{upstream_task}->{downstream_task}",
                    message=str(exc),
                )
            )

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

    overall_severity = Severity.NORMAL
    all_drift_results = [
        *drift_results.values(),
        *handoff_drift_results.values(),
    ]

    if all_drift_results:
        overall_severity = max(
            all_drift_results,
            key=lambda result: SEVERITY_SCORE[result.severity],
        ).severity

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
        diagnostics=diagnostics,
        policy=policy,
        change_point_results=change_point_results,
        handoff_change_point_results=handoff_change_point_results,
        trend_results=trend_results,
        handoff_trend_results=handoff_trend_results,
    )
