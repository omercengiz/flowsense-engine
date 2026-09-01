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


def _build_reverse_dependencies(
    dependencies: dict[str, list[str]],
) -> dict[str, list[str]]:
    reverse_dependencies: dict[str, list[str]] = {
        task_id: [] for task_id in dependencies
    }

    for upstream_task, downstream_tasks in dependencies.items():
        for downstream_task in downstream_tasks:
            reverse_dependencies.setdefault(downstream_task, []).append(upstream_task)

    return reverse_dependencies


def _has_anomalous_upstream(
    task_id: str,
    drift_results: dict[str, DriftResult],
    reverse_dependencies: dict[str, list[str]],
    visited: set[str] | None = None,
) -> bool:
    if visited is None:
        visited = set()

    if task_id in visited:
        return False

    visited = {*visited, task_id}

    for upstream_task in reverse_dependencies.get(task_id, []):
        upstream_drift = drift_results.get(upstream_task)

        if upstream_drift is None or upstream_drift.severity == "NORMAL":
            continue

        if upstream_drift.severity in {
            "HIGH",
            "CRITICAL",
        }:
            return True

        if _has_anomalous_upstream(
            task_id=upstream_task,
            drift_results=drift_results,
            reverse_dependencies=reverse_dependencies,
            visited=visited,
        ):
            return True

    return False


def _find_propagation_paths(
    origin_task: str,
    current_task: str,
    drift_results: dict[str, DriftResult],
    dependencies: dict[str, list[str]],
    path: list[str],
    visited: set[str],
) -> list[list[str]]:
    paths: list[list[str]] = []

    for downstream_task in dependencies.get(current_task, []):
        if downstream_task in visited:
            continue

        downstream_drift = drift_results.get(downstream_task)

        if downstream_drift is None:
            continue

        if downstream_drift.severity == "NORMAL":
            continue

        next_path = [*path, downstream_task]
        next_visited = {*visited, downstream_task}

        child_paths = _find_propagation_paths(
            origin_task=origin_task,
            current_task=downstream_task,
            drift_results=drift_results,
            dependencies=dependencies,
            path=next_path,
            visited=next_visited,
        )

        if child_paths:
            paths.extend(child_paths)
        else:
            paths.append(next_path)

    return paths


def analyze_propagation(
    drift_results: dict[str, DriftResult],
    dependencies: dict[str, list[str]],
) -> list[PropagationResult]:
    results: list[PropagationResult] = []

    reverse_dependencies = _build_reverse_dependencies(dependencies)

    for task_id, drift in drift_results.items():
        if drift.severity not in {"HIGH", "CRITICAL"}:
            continue

        if _has_anomalous_upstream(
            task_id=task_id,
            drift_results=drift_results,
            reverse_dependencies=reverse_dependencies,
        ):
            continue

        paths = _find_propagation_paths(
            origin_task=task_id,
            current_task=task_id,
            drift_results=drift_results,
            dependencies=dependencies,
            path=[task_id],
            visited={task_id},
        )

        for path in paths:
            affected_tasks = path[1:]

            if not affected_tasks:
                continue

            origin_score = SEVERITY_SCORE[drift.severity]

            downstream_scores = [
                SEVERITY_SCORE[drift_results[task].severity] for task in affected_tasks
            ]

            propagation_score = sum(downstream_scores) / (
                len(downstream_scores) * origin_score
            )

            results.append(
                PropagationResult(
                    origin_task=task_id,
                    affected_tasks=affected_tasks,
                    path=path,
                    propagation_score=propagation_score,
                )
            )

    return results
