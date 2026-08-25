from __future__ import annotations

from dataclasses import dataclass

from flowsense.engine.drift import DriftResult
from flowsense.engine.impact import TaskImpact
from flowsense.engine.propagation import PropagationResult
from flowsense.engine.root_cause import RootCauseResult


@dataclass
class DAGAnalysis:
    dag_id: str
    runs_analyzed: int
    overall_severity: str
    primary_origin: RootCauseResult | None
    drift_results: dict[str, DriftResult]
    handoff_drift_results: dict[tuple[str, str], DriftResult]
    task_impacts: dict[str, TaskImpact]
    propagation_results: list[PropagationResult]
    dependencies: dict[str, list[str]]
