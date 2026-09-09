from __future__ import annotations

from dataclasses import dataclass

from flowsense.domain import (
    AnalysisDiagnostic,
    AnalysisPolicy,
    ChangePointResult,
    DriftResult,
    InsufficientHistoryError,
    Severity,
    TaskImpact,
    TaskRun,
    TrendResult,
)
from flowsense.domain.enums import SEVERITY_SCORE
from flowsense.engine.change_point import detect_change_point
from flowsense.engine.drift import calculate_drift
from flowsense.engine.impact import classify_task_impact
from flowsense.engine.timing import (
    build_handoff_history_with_diagnostics,
    calculate_handoff_drift,
)
from flowsense.engine.trend import detect_trend

TaskId = str
Edge = tuple[str, str]


@dataclass(frozen=True)
class TaskAnalysisStageResult:
    drift_results: dict[TaskId, DriftResult]
    change_point_results: dict[TaskId, ChangePointResult]
    trend_results: dict[TaskId, TrendResult]
    diagnostics: list[AnalysisDiagnostic]


@dataclass(frozen=True)
class HandoffAnalysisStageResult:
    drift_results: dict[Edge, DriftResult]
    change_point_results: dict[Edge, ChangePointResult]
    trend_results: dict[Edge, TrendResult]
    diagnostics: list[AnalysisDiagnostic]


def _detect_change_point(
    subject_id: str,
    values: list[float],
    policy: AnalysisPolicy,
) -> ChangePointResult | None:
    if not policy.change_point_detection_enabled:
        return None

    return detect_change_point(
        subject_id,
        values,
        minimum_segment_size=policy.change_point_minimum_segment_size,
        score_threshold=policy.change_point_score_threshold,
    )


def _detect_trend(
    subject_id: str,
    values: list[float],
    policy: AnalysisPolicy,
) -> TrendResult | None:
    if not policy.trend_detection_enabled:
        return None

    return detect_trend(
        subject_id,
        values,
        minimum_observations=policy.trend_minimum_observations,
        score_threshold=policy.trend_score_threshold,
        minimum_directional_consistency=(policy.trend_minimum_directional_consistency),
    )


def analyze_task_histories(
    duration_history: dict[TaskId, list[float]],
    policy: AnalysisPolicy,
) -> TaskAnalysisStageResult:
    drift_results: dict[TaskId, DriftResult] = {}
    change_point_results: dict[TaskId, ChangePointResult] = {}
    trend_results: dict[TaskId, TrendResult] = {}
    diagnostics: list[AnalysisDiagnostic] = []

    for task_id, durations in duration_history.items():
        change_point = _detect_change_point(task_id, durations, policy)
        if change_point is not None:
            change_point_results[task_id] = change_point

        trend = _detect_trend(task_id, durations, policy)
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

    return TaskAnalysisStageResult(
        drift_results=drift_results,
        change_point_results=change_point_results,
        trend_results=trend_results,
        diagnostics=diagnostics,
    )


def analyze_handoff_histories(
    task_runs: list[TaskRun],
    dependencies: dict[TaskId, list[TaskId]],
    policy: AnalysisPolicy,
) -> HandoffAnalysisStageResult:
    history_result = build_handoff_history_with_diagnostics(
        task_runs=task_runs,
        dependencies=dependencies,
    )
    drift_results: dict[Edge, DriftResult] = {}
    change_point_results: dict[Edge, ChangePointResult] = {}
    trend_results: dict[Edge, TrendResult] = {}
    diagnostics = [
        AnalysisDiagnostic(
            code=diagnostic.code,
            subject_id=f"{diagnostic.upstream_task}->{diagnostic.downstream_task}",
            message=diagnostic.message,
        )
        for diagnostic in history_result.diagnostics
    ]

    for edge, delays in history_result.history.items():
        upstream_task, downstream_task = edge
        subject_id = f"{upstream_task}->{downstream_task}"

        change_point = _detect_change_point(subject_id, delays, policy)
        if change_point is not None:
            change_point_results[edge] = change_point

        trend = _detect_trend(subject_id, delays, policy)
        if trend is not None:
            trend_results[edge] = trend

        try:
            drift_results[edge] = calculate_handoff_drift(
                upstream_task=upstream_task,
                downstream_task=downstream_task,
                handoff_delays=delays,
                policy=policy,
            )
        except InsufficientHistoryError as exc:
            diagnostics.append(
                AnalysisDiagnostic(
                    code="INSUFFICIENT_HANDOFF_HISTORY",
                    subject_id=subject_id,
                    message=str(exc),
                )
            )

    return HandoffAnalysisStageResult(
        drift_results=drift_results,
        change_point_results=change_point_results,
        trend_results=trend_results,
        diagnostics=diagnostics,
    )


def classify_task_impacts(
    task_drift_results: dict[TaskId, DriftResult],
    handoff_drift_results: dict[Edge, DriftResult],
) -> dict[TaskId, TaskImpact]:
    return {
        task_id: classify_task_impact(
            task_id=task_id,
            task_drift=task_drift,
            upstream_handoff_drifts=[
                drift
                for (_upstream, downstream), drift in handoff_drift_results.items()
                if downstream == task_id
            ],
        )
        for task_id, task_drift in task_drift_results.items()
    }


def determine_overall_severity(
    task_drift_results: dict[TaskId, DriftResult],
    handoff_drift_results: dict[Edge, DriftResult],
) -> Severity:
    drift_results = [
        *task_drift_results.values(),
        *handoff_drift_results.values(),
    ]

    if not drift_results:
        return Severity.NORMAL

    return max(
        drift_results,
        key=lambda result: SEVERITY_SCORE[result.severity],
    ).severity
