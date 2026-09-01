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


def test_top_level_api_declares_supported_exports() -> None:
    expected_exports = {
        "AirflowApiError",
        "AirflowClient",
        "AnalysisPolicy",
        "DAGAnalysis",
        "DAGDataSource",
        "Severity",
        "MappedTaskAggregation",
        "TaskRun",
        "__version__",
        "analyze_dag",
    }

    assert expected_exports <= set(flowsense.__all__)
    assert flowsense.__version__
