from __future__ import annotations

from dataclasses import dataclass

from flowsense.engine.drift import DriftResult


@dataclass
class TaskImpact:
    task_id: str
    classification: str
    task_severity: str
    upstream_handoff_severity: str | None


def classify_task_impact(
    task_id: str,
    task_drift: DriftResult,
    upstream_handoff_drifts: list[DriftResult],
) -> TaskImpact:
    anomalous_handoffs = [
        drift
        for drift in upstream_handoff_drifts
        if drift.severity
        in {
            "MEDIUM",
            "HIGH",
            "CRITICAL",
        }
    ]

    task_is_anomalous = task_drift.severity in {
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    }

    if task_is_anomalous and anomalous_handoffs:
        classification = "COMBINED"
    elif task_is_anomalous:
        classification = "OWN_DRIFT"
    elif anomalous_handoffs:
        classification = "INHERITED_DELAY"
    else:
        classification = "NORMAL"

    upstream_handoff_severity = None

    if anomalous_handoffs:
        severity_order = {
            "NORMAL": 0,
            "MEDIUM": 1,
            "HIGH": 2,
            "CRITICAL": 3,
        }

        upstream_handoff_severity = max(
            anomalous_handoffs,
            key=lambda result: severity_order[result.severity],
        ).severity

    return TaskImpact(
        task_id=task_id,
        classification=classification,
        task_severity=task_drift.severity,
        upstream_handoff_severity=upstream_handoff_severity,
    )
