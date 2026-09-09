from flowsense import AnalysisPolicy, DriftResult
from flowsense.application.pipeline import (
    analyze_task_histories,
    determine_overall_severity,
)


def _drift(task_id: str, severity: str) -> DriftResult:
    return DriftResult(
        task_id=task_id,
        baseline=1.0,
        current=2.0,
        mad=0.1,
        robust_z_score=3.0,
        deviation_percent=100.0,
        severity=severity,
    )


def test_task_stage_returns_typed_results_and_diagnostics() -> None:
    result = analyze_task_histories(
        {
            "ready": [1.0, 1.0, 1.0, 1.0, 5.0],
            "short": [1.0, 1.0],
        },
        AnalysisPolicy(),
    )

    assert result.drift_results["ready"].severity == "CRITICAL"
    assert "short" not in result.drift_results
    assert [(item.code, item.subject_id) for item in result.diagnostics] == [
        ("INSUFFICIENT_TASK_HISTORY", "short")
    ]


def test_overall_severity_combines_task_and_handoff_stages() -> None:
    severity = determine_overall_severity(
        {"transform": _drift("transform", "MEDIUM")},
        {("extract", "transform"): _drift("extract->transform", "CRITICAL")},
    )

    assert severity == "CRITICAL"


def test_overall_severity_is_normal_without_drift_results() -> None:
    assert determine_overall_severity({}, {}) == "NORMAL"
