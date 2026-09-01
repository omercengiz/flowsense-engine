from __future__ import annotations

from dataclasses import dataclass

from flowsense.engine.drift import DriftResult
from flowsense.engine.impact import TaskImpact
from flowsense.engine.propagation import PropagationResult


@dataclass
class RootCauseResult:
    task_id: str
    classification: str
    severity: str
    propagation_score: float


SEVERITY_SCORE = {
    "NORMAL": 0,
    "MEDIUM": 1,
    "HIGH": 2,
    "CRITICAL": 3,
}


def _build_reverse_dependencies(
    dependencies: dict[str, list[str]],
) -> dict[str, list[str]]:
    reverse_dependencies: dict[str, list[str]] = {}

    for upstream_task, downstream_tasks in dependencies.items():
        reverse_dependencies.setdefault(upstream_task, [])

        for downstream_task in downstream_tasks:
            reverse_dependencies.setdefault(
                downstream_task,
                [],
            ).append(upstream_task)

    return reverse_dependencies


def _has_candidate_upstream(
    task_id: str,
    candidate_tasks: set[str],
    drift_results: dict[str, DriftResult],
    reverse_dependencies: dict[str, list[str]],
    visited: set[str] | None = None,
) -> bool:
    if visited is None:
        visited = set()

    if task_id in visited:
        return False

    visited.add(task_id)

    for upstream_task in reverse_dependencies.get(task_id, []):
        upstream_drift = drift_results.get(upstream_task)

        if upstream_drift is None or upstream_drift.severity == "NORMAL":
            continue

        if upstream_task in candidate_tasks:
            return True

        if _has_candidate_upstream(
            task_id=upstream_task,
            candidate_tasks=candidate_tasks,
            drift_results=drift_results,
            reverse_dependencies=reverse_dependencies,
            visited=visited,
        ):
            return True

    return False


def select_primary_origin(
    drift_results: dict[str, DriftResult],
    task_impacts: dict[str, TaskImpact],
    dependencies: dict[str, list[str]],
    propagation_results: list[PropagationResult],
) -> RootCauseResult | None:
    candidate_tasks = {
        task_id
        for task_id, impact in task_impacts.items()
        if impact.classification
        in {
            "OWN_DRIFT",
            "COMBINED",
        }
    }

    if not candidate_tasks:
        return None

    reverse_dependencies = _build_reverse_dependencies(
        dependencies,
    )

    root_candidates = {
        task_id
        for task_id in candidate_tasks
        if not _has_candidate_upstream(
            task_id=task_id,
            candidate_tasks=candidate_tasks,
            drift_results=drift_results,
            reverse_dependencies=reverse_dependencies,
        )
    }

    if not root_candidates:
        root_candidates = candidate_tasks

    propagation_scores: dict[str, float] = {}

    for result in propagation_results:
        current_score = propagation_scores.get(
            result.origin_task,
            0.0,
        )

        propagation_scores[result.origin_task] = max(
            current_score,
            result.propagation_score,
        )

    def ranking(task_id: str) -> tuple[float, int]:
        propagation_score = propagation_scores.get(
            task_id,
            0.0,
        )

        drift = drift_results.get(task_id)

        severity_score = SEVERITY_SCORE[drift.severity] if drift else 0

        return (
            propagation_score,
            severity_score,
        )

    primary_task = max(
        root_candidates,
        key=ranking,
    )

    impact = task_impacts[primary_task]
    drift = drift_results[primary_task]

    return RootCauseResult(
        task_id=primary_task,
        classification=impact.classification,
        severity=drift.severity,
        propagation_score=propagation_scores.get(
            primary_task,
            0.0,
        ),
    )
