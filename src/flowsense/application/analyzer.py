from __future__ import annotations

from flowsense.application.analysis_engine import DEFAULT_DAG_ANALYSIS_ENGINE
from flowsense.application.ports import DAGAnalysisEngine, DAGDataSource
from flowsense.domain import DEFAULT_ANALYSIS_POLICY, AnalysisPolicy, DAGAnalysis


def analyze_dag(
    dag_id: str,
    source: DAGDataSource,
    policy: AnalysisPolicy = DEFAULT_ANALYSIS_POLICY,
    analysis_engine: DAGAnalysisEngine = DEFAULT_DAG_ANALYSIS_ENGINE,
) -> DAGAnalysis:
    task_runs = source.collect_task_runs(dag_id)
    dependencies = source.get_dag_dependencies(dag_id)
    return analysis_engine.analyze(
        dag_id=dag_id,
        task_runs=task_runs,
        dependencies=dependencies,
        policy=policy,
    )
