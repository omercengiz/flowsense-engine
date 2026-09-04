from flowsense import (
    AnalysisDiagnostic,
    ChangePointResult,
    DAGAnalysis,
    DriftResult,
    PropagationResult,
    TrendResult,
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


def test_summarizes_dag_analysis_results() -> None:
    analysis = DAGAnalysis(
        dag_id="demo",
        runs_analyzed=8,
        overall_severity="CRITICAL",
        primary_origin=None,
        drift_results={
            "extract": _drift("extract", "NORMAL"),
            "transform": _drift("transform", "CRITICAL"),
            "load": _drift("load", "MEDIUM"),
        },
        handoff_drift_results={
            ("extract", "transform"): _drift("extract->transform", "HIGH"),
            ("transform", "load"): _drift("transform->load", "NORMAL"),
        },
        task_impacts={},
        propagation_results=[
            PropagationResult(
                origin_task="transform",
                affected_tasks=["load", "notify"],
                path=["transform", "load", "notify"],
                propagation_score=0.7,
            ),
            PropagationResult(
                origin_task="transform",
                affected_tasks=["load"],
                path=["transform", "load"],
                propagation_score=0.8,
            ),
        ],
        dependencies={
            "extract": ["transform"],
            "transform": ["load"],
            "load": ["notify"],
            "notify": [],
        },
        diagnostics=[
            AnalysisDiagnostic(
                code="INSUFFICIENT_TASK_HISTORY",
                subject_id="notify",
                message="Not enough observations.",
            )
        ],
        change_point_results={
            "transform": ChangePointResult(
                subject_id="transform",
                change_index=4,
                before_median=1.0,
                after_median=2.0,
                change_percent=100.0,
                score=5.0,
                direction="INCREASE",
            )
        },
        handoff_change_point_results={
            ("extract", "transform"): ChangePointResult(
                subject_id="extract->transform",
                change_index=4,
                before_median=1.0,
                after_median=2.0,
                change_percent=100.0,
                score=5.0,
                direction="INCREASE",
            )
        },
        trend_results={
            "load": TrendResult(
                subject_id="load",
                direction="INCREASING",
                slope_per_observation=0.2,
                estimated_change=1.4,
                change_percent=70.0,
                score=4.0,
                directional_consistency=0.8,
                observations=8,
            )
        },
    )

    summary = analysis.summary

    assert summary.total_tasks == 4
    assert summary.analyzed_tasks == 3
    assert summary.analysis_coverage_percent == 75.0
    assert summary.normal_tasks == 1
    assert summary.medium_tasks == 1
    assert summary.high_tasks == 0
    assert summary.critical_tasks == 1
    assert summary.anomalous_tasks == 2
    assert summary.anomalous_handoffs == 1
    assert summary.affected_tasks == 2
    assert summary.change_points == 2
    assert summary.trends == 1
    assert summary.diagnostics == 1


def test_empty_analysis_has_zero_summary() -> None:
    analysis = DAGAnalysis(
        dag_id="empty",
        runs_analyzed=0,
        overall_severity="NORMAL",
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )

    assert analysis.summary.total_tasks == 0
    assert analysis.summary.analyzed_tasks == 0
    assert analysis.summary.analysis_coverage_percent == 0.0
