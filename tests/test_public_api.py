from unittest.mock import MagicMock

import flowsense


class EmptyDataSource:
    def collect_task_runs(self, dag_id: str) -> list[flowsense.TaskRun]:
        return []

    def get_dag_dependencies(self, dag_id: str) -> dict[str, list[str]]:
        return {}


def test_top_level_api_analyzes_custom_data_source() -> None:
    analysis = flowsense.analyze_dag(
        dag_id="demo",
        source=EmptyDataSource(),
    )

    assert analysis.dag_id == "demo"
    assert analysis.overall_severity is flowsense.Severity.NORMAL


def test_top_level_api_accepts_custom_analysis_engine() -> None:
    engine = MagicMock(spec=flowsense.DAGAnalysisEngine)
    engine.analyze.return_value = flowsense.DAGAnalysis(
        dag_id="demo",
        runs_analyzed=0,
        overall_severity=flowsense.Severity.NORMAL,
        primary_origin=None,
        drift_results={},
        handoff_drift_results={},
        task_impacts={},
        propagation_results=[],
        dependencies={},
    )

    analysis = flowsense.analyze_dag(
        dag_id="demo",
        source=EmptyDataSource(),
        analysis_engine=engine,
    )

    assert analysis is engine.analyze.return_value


def test_top_level_api_declares_supported_exports() -> None:
    expected_exports = {
        "AirflowApiError",
        "AirflowClient",
        "AirflowDagRunNotFoundError",
        "AirflowDataError",
        "ANALYSIS_POLICY_SCHEMA_VERSION",
        "ANALYSIS_SCHEMA_VERSION",
        "BATCH_ANALYSIS_SCHEMA_VERSION",
        "DEFAULT_DAG_ANALYSIS_ENGINE",
        "AnalysisPolicy",
        "AnalysisPolicyDocument",
        "AnalysisRequest",
        "AnalysisDocument",
        "AnalysisMetricsSink",
        "BatchAnalysisDocument",
        "BatchAnalysisResult",
        "ChangeDirection",
        "ChangePointResult",
        "ConfigurationError",
        "DAGAnalysis",
        "DAGAnalysisEngine",
        "DAGAnalysisSummary",
        "DAGDataSource",
        "DefaultDAGAnalysisEngine",
        "FlowSenseClient",
        "Severity",
        "MappedTaskAggregation",
        "TaskRun",
        "TrendDirection",
        "TrendResult",
        "__version__",
        "analysis_json_schema",
        "analysis_policy_json_schema",
        "batch_analysis_json_schema",
        "analyze_dag",
        "build_analysis_document",
        "build_batch_analysis_document",
        "serialize_batch_analysis",
        "serialize_analysis",
    }

    assert expected_exports <= set(flowsense.__all__)
    assert flowsense.__version__
