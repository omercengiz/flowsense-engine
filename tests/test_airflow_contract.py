from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx
import pytest

from flowsense.application import AnalysisRequest, AnalyzeDAG
from flowsense.infrastructure.airflow import AirflowClient, AirflowConfig

FIXTURES = Path(__file__).parent / "fixtures" / "airflow"


def _load_fixture(api_version: str, name: str) -> dict[str, Any]:
    path = FIXTURES / api_version / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.airflow_contract
def test_airflow_rest_contract_supports_complete_analysis() -> None:
    airflow_major = os.getenv("FLOWSENSE_AIRFLOW_CONTRACT_VERSION")
    if airflow_major is None:
        pytest.skip("Set FLOWSENSE_AIRFLOW_CONTRACT_VERSION to 2 or 3")
    if airflow_major not in {"2", "3"}:
        pytest.fail("FLOWSENSE_AIRFLOW_CONTRACT_VERSION must be 2 or 3")

    api_version = "v1" if airflow_major == "2" else "v2"
    auth_mode = "basic" if airflow_major == "2" else "token"
    requests: list[httpx.Request] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"access_token": "contract-token"})
        if "/~/" in request.url.path or request.url.path.endswith("/dagRuns/list"):
            return httpx.Response(404)
        if request.url.path.endswith("/dagRuns"):
            return httpx.Response(200, json=_load_fixture(api_version, "dag_runs"))
        if request.url.path.endswith("/taskInstances"):
            return httpx.Response(
                200,
                json=_load_fixture(api_version, "task_instances"),
            )
        if request.url.path.endswith("/tasks"):
            return httpx.Response(200, json=_load_fixture(api_version, "tasks"))
        return httpx.Response(404)

    http_client = httpx.Client(transport=httpx.MockTransport(handle_request))
    client = AirflowClient(
        config=AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
            api_version=api_version,
            auth_mode=auth_mode,
            max_retries=0,
        ),
        http_client=http_client,
    )

    analysis = AnalyzeDAG(lambda _: client).execute(
        AnalysisRequest(dag_id="flowsense_demo")
    )

    assert analysis.runs_analyzed == 5
    assert analysis.current_dag_run_id == "run_5"
    assert analysis.summary.total_tasks == 3
    assert analysis.summary.analyzed_tasks == 3
    assert analysis.dependencies == {
        "extract": ["transform"],
        "transform": ["load"],
        "load": [],
    }

    api_requests = [
        request for request in requests if request.url.path != "/auth/token"
    ]
    assert api_requests
    assert all(f"/api/{api_version}/" in request.url.path for request in api_requests)
    if airflow_major == "2":
        assert not any(request.url.path == "/auth/token" for request in requests)
        assert all(
            request.headers.get("Authorization", "").startswith("Basic ")
            for request in api_requests
        )
    else:
        token_requests = [
            request for request in requests if request.url.path == "/auth/token"
        ]
        assert len(token_requests) == 1
        assert all(
            request.headers["Authorization"] == "Bearer contract-token"
            for request in api_requests
        )
