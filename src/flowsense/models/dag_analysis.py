from __future__ import annotations

from dataclasses import dataclass

from flowsense.engine.drift import DriftResult
from flowsense.engine.propagation import PropagationResult


@dataclass
class DAGAnalysis:
    dag_id: str
    runs_analyzed: int
    overall_severity: str
    primary_origin: str | None
    drift_results: dict[str, DriftResult]
    propagation_results: list[PropagationResult]
    dependencies: dict[str, list[str]]
