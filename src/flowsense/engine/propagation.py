from __future__ import annotations

from dataclasses import dataclass

from flowsense.engine.drift import DriftResult

SEVERITY_SCORE = {
    "NORMAL": 0,
    "MEDIUM": 1,
    "HIGH": 2,
    "CRITICAL": 3,
}


@dataclass
class PropagationResult:
    origin_task: str
    affected_tasks: list[str]
    path: list[str]
    propagation_score: float


def analyze_propagation(
    drift_results: dict[str, DriftResult],
    dependencies: dict[str, list[str]],
) -> list[PropagationResult]:
    results: list[PropagationResult] = []

    for task_id, drift in drift_results.items():
        if drift.severity not in {"HIGH", "CRITICAL"}:
            continue

        downstream = dependencies.get(task_id, [])

        affected: list[str] = []

        for downstream_task in downstream:
            downstream_drift = drift_results.get(downstream_task)

            if downstream_drift is None:
                continue

            if downstream_drift.severity != "NORMAL":
                affected.append(downstream_task)

        if not affected:
            continue

        origin_score = SEVERITY_SCORE[drift.severity]

        downstream_scores = [
            SEVERITY_SCORE[drift_results[task].severity] for task in affected
        ]

        propagation_score = sum(downstream_scores) / (
            len(downstream_scores) * origin_score
        )

        results.append(
            PropagationResult(
                origin_task=task_id,
                affected_tasks=affected,
                path=[task_id, *affected],
                propagation_score=propagation_score,
            )
        )

    return results
