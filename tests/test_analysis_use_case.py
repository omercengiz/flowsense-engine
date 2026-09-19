from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

from flowsense.application import AnalysisRequest, AnalyzeDAG, DAGDataSource


def test_analyze_dag_use_case_coordinates_request_and_data_source() -> None:
    source = MagicMock(spec=DAGDataSource)
    source.collect_task_runs.return_value = []
    source.get_dag_dependencies.return_value = {}
    received_requests: list[AnalysisRequest] = []

    @contextmanager
    def source_factory(
        request: AnalysisRequest,
    ) -> Iterator[DAGDataSource]:
        received_requests.append(request)
        yield source

    request = AnalysisRequest(dag_id="demo")

    analysis = AnalyzeDAG(source_factory).execute(request)

    assert received_requests == [request]
    assert analysis.dag_id == "demo"
    source.collect_task_runs.assert_called_once_with("demo")
    source.get_dag_dependencies.assert_called_once_with("demo")
