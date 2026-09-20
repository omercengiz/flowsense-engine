from __future__ import annotations

from flowsense.application.batch import BatchAnalysisResult
from flowsense.application.output import (
    AnalysisDocument,
    AnalysisPolicyOutput,
    AnalysisSummaryOutput,
    BatchAnalysisDocument,
    BatchFailureOutput,
    ChangePointOutput,
    DiagnosticOutput,
    DriftOutput,
    PropagationOutput,
    RootCauseOutput,
    TaskImpactOutput,
    TrendOutput,
)
from flowsense.domain import DAGAnalysis, DriftResult


def _drift_output(result: DriftResult) -> DriftOutput:
    return DriftOutput(
        baseline=result.baseline,
        current=result.current,
        mad=result.mad,
        robust_z_score=result.robust_z_score,
        deviation_percent=result.deviation_percent,
        severity=result.severity,
    )


def build_analysis_document(analysis: DAGAnalysis) -> AnalysisDocument:
    """Build and validate the typed public output document."""
    summary = analysis.summary

    return AnalysisDocument(
        dag_id=analysis.dag_id,
        current_dag_run_id=analysis.current_dag_run_id,
        runs_analyzed=analysis.runs_analyzed,
        overall_severity=analysis.overall_severity,
        summary=AnalysisSummaryOutput(
            total_tasks=summary.total_tasks,
            analyzed_tasks=summary.analyzed_tasks,
            analysis_coverage_percent=summary.analysis_coverage_percent,
            normal_tasks=summary.normal_tasks,
            medium_tasks=summary.medium_tasks,
            high_tasks=summary.high_tasks,
            critical_tasks=summary.critical_tasks,
            anomalous_tasks=summary.anomalous_tasks,
            anomalous_handoffs=summary.anomalous_handoffs,
            affected_tasks=summary.affected_tasks,
            change_points=summary.change_points,
            trends=summary.trends,
            diagnostics=summary.diagnostics,
        ),
        policy=AnalysisPolicyOutput(
            minimum_history=analysis.policy.minimum_history,
            baseline_window=analysis.policy.baseline_window,
            medium_threshold=analysis.policy.medium_threshold,
            high_threshold=analysis.policy.high_threshold,
            critical_threshold=analysis.policy.critical_threshold,
            mapped_task_aggregation=analysis.policy.mapped_task_aggregation,
            change_point_detection_enabled=(
                analysis.policy.change_point_detection_enabled
            ),
            change_point_minimum_segment_size=(
                analysis.policy.change_point_minimum_segment_size
            ),
            change_point_score_threshold=(analysis.policy.change_point_score_threshold),
            trend_detection_enabled=analysis.policy.trend_detection_enabled,
            trend_minimum_observations=analysis.policy.trend_minimum_observations,
            trend_score_threshold=analysis.policy.trend_score_threshold,
            trend_minimum_directional_consistency=(
                analysis.policy.trend_minimum_directional_consistency
            ),
        ),
        primary_origin=(
            RootCauseOutput(
                task_id=analysis.primary_origin.task_id,
                classification=analysis.primary_origin.classification,
                severity=analysis.primary_origin.severity,
                propagation_score=analysis.primary_origin.propagation_score,
            )
            if analysis.primary_origin
            else None
        ),
        drift_results={
            task_id: _drift_output(result)
            for task_id, result in analysis.drift_results.items()
        },
        change_point_results={
            task_id: ChangePointOutput(
                change_index=result.change_index,
                before_median=result.before_median,
                after_median=result.after_median,
                change_percent=result.change_percent,
                score=result.score,
                direction=result.direction,
            )
            for task_id, result in analysis.change_point_results.items()
        },
        trend_results={
            task_id: TrendOutput(
                direction=result.direction,
                slope_per_observation=result.slope_per_observation,
                estimated_change=result.estimated_change,
                change_percent=result.change_percent,
                score=result.score,
                directional_consistency=result.directional_consistency,
                observations=result.observations,
            )
            for task_id, result in analysis.trend_results.items()
        },
        handoff_drift_results={
            f"{upstream}->{downstream}": _drift_output(result)
            for (upstream, downstream), result in analysis.handoff_drift_results.items()
        },
        handoff_change_point_results={
            f"{upstream}->{downstream}": ChangePointOutput(
                change_index=result.change_index,
                before_median=result.before_median,
                after_median=result.after_median,
                change_percent=result.change_percent,
                score=result.score,
                direction=result.direction,
            )
            for (
                upstream,
                downstream,
            ), result in analysis.handoff_change_point_results.items()
        },
        handoff_trend_results={
            f"{upstream}->{downstream}": TrendOutput(
                direction=result.direction,
                slope_per_observation=result.slope_per_observation,
                estimated_change=result.estimated_change,
                change_percent=result.change_percent,
                score=result.score,
                directional_consistency=result.directional_consistency,
                observations=result.observations,
            )
            for (upstream, downstream), result in analysis.handoff_trend_results.items()
        },
        task_impacts={
            task_id: TaskImpactOutput(
                classification=impact.classification,
                task_severity=impact.task_severity,
                upstream_handoff_severity=impact.upstream_handoff_severity,
            )
            for task_id, impact in analysis.task_impacts.items()
        },
        propagation_results=[
            PropagationOutput(
                origin_task=result.origin_task,
                affected_tasks=result.affected_tasks,
                path=result.path,
                propagation_score=result.propagation_score,
            )
            for result in analysis.propagation_results
        ],
        dependencies=analysis.dependencies,
        diagnostics=[
            DiagnosticOutput(
                code=diagnostic.code,
                subject_id=diagnostic.subject_id,
                message=diagnostic.message,
            )
            for diagnostic in analysis.diagnostics
        ],
    )


def serialize_analysis(analysis: DAGAnalysis) -> dict[str, object]:
    """Serialize a DAG analysis to the versioned public output schema."""
    return build_analysis_document(analysis).model_dump(mode="json")


def analysis_json_schema() -> dict[str, object]:
    """Return the JSON Schema for the current analysis output contract."""
    return AnalysisDocument.model_json_schema()


def build_batch_analysis_document(
    result: BatchAnalysisResult,
) -> BatchAnalysisDocument:
    """Build and validate the typed public batch output document."""
    return BatchAnalysisDocument(
        requested_dag_ids=list(result.requested_dag_ids),
        successful_count=result.successful_count,
        failed_count=result.failed_count,
        analyses={
            dag_id: build_analysis_document(analysis)
            for dag_id, analysis in result.analyses.items()
        },
        failures={
            dag_id: BatchFailureOutput(
                error_type=type(failure).__name__,
                message=str(failure),
            )
            for dag_id, failure in result.failures.items()
        },
    )


def serialize_batch_analysis(result: BatchAnalysisResult) -> dict[str, object]:
    """Serialize batch results to the versioned public output schema."""
    return build_batch_analysis_document(result).model_dump(mode="json")


def batch_analysis_json_schema() -> dict[str, object]:
    """Return the JSON Schema for the current batch output contract."""
    return BatchAnalysisDocument.model_json_schema()
