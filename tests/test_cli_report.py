from io import StringIO

from rich.console import Console

from flowsense.cli.report import render_analysis
from flowsense.domain import (
    AnalysisDiagnostic,
    ChangePointResult,
    DAGAnalysis,
    DriftResult,
    PropagationResult,
    RootCauseResult,
    TaskImpact,
    TrendResult,
)


def test_renders_complete_analysis_report() -> None:
    task_drift = DriftResult(
        task_id="transform",
        baseline=3.0,
        current=9.0,
        mad=0.2,
        robust_z_score=8.0,
        deviation_percent=200.0,
        severity="CRITICAL",
    )
    handoff_drift = DriftResult(
        task_id="extract->transform",
        baseline=1.0,
        current=4.0,
        mad=0.1,
        robust_z_score=7.0,
        deviation_percent=300.0,
        severity="HIGH",
    )
    analysis = DAGAnalysis(
        dag_id="demo",
        runs_analyzed=8,
        overall_severity="CRITICAL",
        primary_origin=RootCauseResult(
            task_id="transform",
            classification="OWN_DRIFT",
            severity="CRITICAL",
            propagation_score=0.75,
        ),
        drift_results={"transform": task_drift},
        handoff_drift_results={("extract", "transform"): handoff_drift},
        task_impacts={
            "transform": TaskImpact(
                task_id="transform",
                classification="COMBINED",
                task_severity="CRITICAL",
                upstream_handoff_severity="HIGH",
            )
        },
        propagation_results=[
            PropagationResult(
                origin_task="transform",
                affected_tasks=["load"],
                path=["transform", "load"],
                propagation_score=0.75,
            )
        ],
        dependencies={"transform": ["load"], "load": []},
        diagnostics=[
            AnalysisDiagnostic(
                code="INSUFFICIENT_TASK_HISTORY",
                subject_id="load",
                message="Not enough observations.",
            )
        ],
        change_point_results={
            "transform": ChangePointResult(
                subject_id="transform",
                change_index=4,
                before_median=3.0,
                after_median=6.0,
                change_percent=100.0,
                score=5.0,
                direction="INCREASE",
            )
        },
        trend_results={
            "transform": TrendResult(
                subject_id="transform",
                direction="INCREASING",
                slope_per_observation=1.0,
                estimated_change=7.0,
                change_percent=233.3,
                score=5.0,
                directional_consistency=1.0,
                observations=8,
            )
        },
    )
    output = StringIO()
    console = Console(file=output, width=160, color_system=None)

    render_analysis(console, analysis)

    report = output.getvalue()
    assert "FlowSense Analysis" in report
    assert "Task coverage" in report
    assert "1/3 (33.3%)" in report
    assert "Structural signals" in report
    assert "Task Drift" in report
    assert "Handoff Drift" in report
    assert "Change Points" in report
    assert "Trends" in report
    assert "Propagation" in report
    assert "Diagnostics" in report
    assert "transform -> load" in report
    assert "INSUFFICIENT_TASK_HISTORY" in report


def test_renders_empty_task_drift_table_without_optional_sections() -> None:
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
    output = StringIO()
    console = Console(file=output, width=120, color_system=None)

    render_analysis(console, analysis)

    report = output.getvalue()
    assert "FlowSense Analysis" in report
    assert "Task Drift" in report
    assert "Handoff Drift" not in report
    assert "Diagnostics" not in report
