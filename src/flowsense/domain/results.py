from __future__ import annotations

from dataclasses import dataclass, field

from flowsense.domain.enums import ChangeDirection, ImpactClassification, Severity
from flowsense.domain.policy import DEFAULT_ANALYSIS_POLICY, AnalysisPolicy


@dataclass(frozen=True)
class DriftResult:
    task_id: str
    baseline: float
    current: float
    mad: float
    robust_z_score: float
    deviation_percent: float
    severity: Severity


@dataclass(frozen=True)
class ChangePointResult:
    subject_id: str
    change_index: int
    before_median: float
    after_median: float
    change_percent: float | None
    score: float
    direction: ChangeDirection


@dataclass(frozen=True)
class TaskImpact:
    task_id: str
    classification: ImpactClassification
    task_severity: Severity
    upstream_handoff_severity: Severity | None


@dataclass(frozen=True)
class PropagationResult:
    origin_task: str
    affected_tasks: list[str]
    path: list[str]
    propagation_score: float


@dataclass(frozen=True)
class RootCauseResult:
    task_id: str
    classification: ImpactClassification
    severity: Severity
    propagation_score: float


@dataclass(frozen=True)
class AnalysisDiagnostic:
    code: str
    subject_id: str
    message: str


@dataclass
class DAGAnalysis:
    dag_id: str
    runs_analyzed: int
    overall_severity: Severity
    primary_origin: RootCauseResult | None
    drift_results: dict[str, DriftResult]
    handoff_drift_results: dict[tuple[str, str], DriftResult]
    task_impacts: dict[str, TaskImpact]
    propagation_results: list[PropagationResult]
    dependencies: dict[str, list[str]]
    diagnostics: list[AnalysisDiagnostic] = field(default_factory=list)
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY
    change_point_results: dict[str, ChangePointResult] = field(default_factory=dict)
    handoff_change_point_results: dict[tuple[str, str], ChangePointResult] = field(
        default_factory=dict
    )
