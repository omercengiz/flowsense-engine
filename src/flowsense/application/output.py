from typing import Literal

from pydantic import BaseModel, ConfigDict

from flowsense.domain import (
    ChangeDirection,
    DriftDirection,
    ImpactClassification,
    MappedTaskAggregation,
    Severity,
    TrendDirection,
)

ANALYSIS_SCHEMA_VERSION = "1.2"
BATCH_ANALYSIS_SCHEMA_VERSION = "1.0"


class OutputModel(BaseModel):
    """Base model for the versioned public analysis contract."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class AnalysisSummaryOutput(OutputModel):
    total_tasks: int
    analyzed_tasks: int
    analysis_coverage_percent: float
    normal_tasks: int
    medium_tasks: int
    high_tasks: int
    critical_tasks: int
    anomalous_tasks: int
    anomalous_handoffs: int
    affected_tasks: int
    change_points: int
    trends: int
    diagnostics: int


class AnalysisPolicyOutput(OutputModel):
    minimum_history: int
    baseline_window: int | None
    medium_threshold: float
    high_threshold: float
    critical_threshold: float
    minimum_relative_dispersion: float
    minimum_absolute_dispersion: float
    mapped_task_aggregation: MappedTaskAggregation
    change_point_detection_enabled: bool
    change_point_minimum_segment_size: int
    change_point_score_threshold: float
    trend_detection_enabled: bool
    trend_minimum_observations: int
    trend_score_threshold: float
    trend_minimum_directional_consistency: float


class RootCauseOutput(OutputModel):
    task_id: str
    classification: ImpactClassification
    severity: Severity
    propagation_score: float


class DriftOutput(OutputModel):
    baseline: float
    current: float
    mad: float
    effective_mad: float
    robust_z_score: float
    deviation_percent: float
    severity: Severity
    direction: DriftDirection


class ChangePointOutput(OutputModel):
    change_index: int
    before_median: float
    after_median: float
    change_percent: float | None
    score: float
    direction: ChangeDirection


class TrendOutput(OutputModel):
    direction: TrendDirection
    slope_per_observation: float
    estimated_change: float
    change_percent: float | None
    score: float
    directional_consistency: float
    observations: int


class TaskImpactOutput(OutputModel):
    classification: ImpactClassification
    task_severity: Severity
    upstream_handoff_severity: Severity | None


class PropagationOutput(OutputModel):
    origin_task: str
    affected_tasks: list[str]
    path: list[str]
    propagation_score: float


class DiagnosticOutput(OutputModel):
    code: str
    subject_id: str
    message: str


class AnalysisDocument(OutputModel):
    """Typed representation of the FlowSense analysis output schema."""

    schema_version: Literal["1.2"] = ANALYSIS_SCHEMA_VERSION
    dag_id: str
    current_dag_run_id: str | None
    runs_analyzed: int
    overall_severity: Severity
    summary: AnalysisSummaryOutput
    policy: AnalysisPolicyOutput
    primary_origin: RootCauseOutput | None
    drift_results: dict[str, DriftOutput]
    change_point_results: dict[str, ChangePointOutput]
    trend_results: dict[str, TrendOutput]
    handoff_drift_results: dict[str, DriftOutput]
    handoff_change_point_results: dict[str, ChangePointOutput]
    handoff_trend_results: dict[str, TrendOutput]
    task_impacts: dict[str, TaskImpactOutput]
    propagation_results: list[PropagationOutput]
    dependencies: dict[str, list[str]]
    diagnostics: list[DiagnosticOutput]


class BatchFailureOutput(OutputModel):
    error_type: str
    message: str


class BatchAnalysisDocument(OutputModel):
    """Typed representation of the FlowSense batch output schema."""

    schema_version: Literal["1.0"] = BATCH_ANALYSIS_SCHEMA_VERSION
    requested_dag_ids: list[str]
    successful_count: int
    failed_count: int
    analyses: dict[str, AnalysisDocument]
    failures: dict[str, BatchFailureOutput]
