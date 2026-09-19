from flowsense.application.pipeline import (
    analyze_handoff_histories,
    analyze_task_histories,
    classify_task_impacts,
    determine_overall_severity,
)
from flowsense.domain import AnalysisPolicy, DAGAnalysis, TaskRun
from flowsense.engine.history import build_duration_history
from flowsense.engine.propagation import analyze_propagation
from flowsense.engine.root_cause import select_primary_origin


class DefaultDAGAnalysisEngine:
    """Default stateless implementation of the DAG analysis domain workflow."""

    def analyze(
        self,
        dag_id: str,
        task_runs: list[TaskRun],
        dependencies: dict[str, list[str]],
        policy: AnalysisPolicy,
    ) -> DAGAnalysis:
        duration_history = build_duration_history(
            task_runs,
            aggregation=policy.mapped_task_aggregation,
        )

        task_analysis = analyze_task_histories(duration_history, policy)
        handoff_analysis = analyze_handoff_histories(task_runs, dependencies, policy)
        task_impacts = classify_task_impacts(
            task_analysis.drift_results,
            handoff_analysis.drift_results,
        )
        propagation_results = analyze_propagation(
            drift_results=task_analysis.drift_results,
            dependencies=dependencies,
        )
        primary_origin = select_primary_origin(
            drift_results=task_analysis.drift_results,
            task_impacts=task_impacts,
            dependencies=dependencies,
            propagation_results=propagation_results,
        )

        return DAGAnalysis(
            dag_id=dag_id,
            runs_analyzed=len({run.dag_run_id for run in task_runs}),
            current_dag_run_id=(task_runs[-1].dag_run_id if task_runs else None),
            overall_severity=determine_overall_severity(
                task_analysis.drift_results,
                handoff_analysis.drift_results,
            ),
            primary_origin=primary_origin,
            drift_results=task_analysis.drift_results,
            handoff_drift_results=handoff_analysis.drift_results,
            task_impacts=task_impacts,
            propagation_results=propagation_results,
            dependencies=dependencies,
            diagnostics=[
                *task_analysis.diagnostics,
                *handoff_analysis.diagnostics,
            ],
            policy=policy,
            change_point_results=task_analysis.change_point_results,
            handoff_change_point_results=handoff_analysis.change_point_results,
            trend_results=task_analysis.trend_results,
            handoff_trend_results=handoff_analysis.trend_results,
        )


DEFAULT_DAG_ANALYSIS_ENGINE = DefaultDAGAnalysisEngine()
