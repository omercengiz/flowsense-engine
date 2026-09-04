from __future__ import annotations

from dataclasses import dataclass, field

from flowsense.domain.enums import (
    ChangeDirection,
    ImpactClassification,
    Severity,
    TrendDirection,
)
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
class TrendResult:
    subject_id: str
    direction: TrendDirection
    slope_per_observation: float
    estimated_change: float
    change_percent: float | None
    score: float
    directional_consistency: float
    observations: int


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


@dataclass(frozen=True)
class DAGAnalysisSummary:
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
    trend_results: dict[str, TrendResult] = field(default_factory=dict)
    handoff_trend_results: dict[tuple[str, str], TrendResult] = field(
        default_factory=dict
    )

    @property
    def summary(self) -> DAGAnalysisSummary:
        task_ids = {
            *self.dependencies,
            *(task for tasks in self.dependencies.values() for task in tasks),
            *self.drift_results,
            *self.task_impacts,
            *self.change_point_results,
            *self.trend_results,
            *(
                task
                for edge in (
                    *self.handoff_drift_results,
                    *self.handoff_change_point_results,
                    *self.handoff_trend_results,
                )
                for task in edge
            ),
            *(
                task
                for result in self.propagation_results
                for task in (result.origin_task, *result.affected_tasks)
            ),
        }
        severity_counts = {
            severity: sum(
                result.severity == severity for result in self.drift_results.values()
            )
            for severity in Severity
        }
        analyzed_tasks = len(self.drift_results)
        total_tasks = len(task_ids)
        anomalous_severities = {
            Severity.MEDIUM,
            Severity.HIGH,
            Severity.CRITICAL,
        }

        return DAGAnalysisSummary(
            total_tasks=total_tasks,
            analyzed_tasks=analyzed_tasks,
            analysis_coverage_percent=(
                analyzed_tasks / total_tasks * 100 if total_tasks else 0.0
            ),
            normal_tasks=severity_counts[Severity.NORMAL],
            medium_tasks=severity_counts[Severity.MEDIUM],
            high_tasks=severity_counts[Severity.HIGH],
            critical_tasks=severity_counts[Severity.CRITICAL],
            anomalous_tasks=sum(
                result.severity in anomalous_severities
                for result in self.drift_results.values()
            ),
            anomalous_handoffs=sum(
                result.severity in anomalous_severities
                for result in self.handoff_drift_results.values()
            ),
            affected_tasks=len(
                {
                    task
                    for result in self.propagation_results
                    for task in result.affected_tasks
                }
            ),
            change_points=(
                len(self.change_point_results) + len(self.handoff_change_point_results)
            ),
            trends=len(self.trend_results) + len(self.handoff_trend_results),
            diagnostics=len(self.diagnostics),
        )
