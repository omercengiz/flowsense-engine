from unittest.mock import MagicMock, call, patch

import pytest

from flowsense.config import AirflowConfig
from flowsense.infrastructure.airflow.client import PAGE_SIZE, AirflowClient


@pytest.fixture
def client() -> AirflowClient:
    with patch(
        "flowsense.infrastructure.airflow.client.get_airflow_config",
        return_value=AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
        ),
    ):
        airflow_client = AirflowClient()

    airflow_client._token = "token"
    return airflow_client


@patch("flowsense.infrastructure.airflow.client.httpx.get")
def test_get_dag_runs_collects_all_pages(
    mock_get: MagicMock,
    client: AirflowClient,
) -> None:
    first_page = MagicMock()
    first_page.json.return_value = {
        "dag_runs": [{"dag_run_id": f"run_{index}"} for index in range(PAGE_SIZE)],
        "total_entries": PAGE_SIZE + 1,
    }

    second_page = MagicMock()
    second_page.json.return_value = {
        "dag_runs": [{"dag_run_id": f"run_{PAGE_SIZE}"}],
        "total_entries": PAGE_SIZE + 1,
    }

    mock_get.side_effect = [first_page, second_page]

    result = client.get_dag_runs("demo")

    assert len(result["dag_runs"]) == PAGE_SIZE + 1
    assert result["total_entries"] == PAGE_SIZE + 1
    assert mock_get.call_args_list == [
        call(
            "http://airflow.test/api/v2/dags/demo/dagRuns",
            headers={
                "Authorization": "Bearer token",
                "Accept": "application/json",
            },
            params={"limit": PAGE_SIZE, "offset": 0},
            timeout=10.0,
        ),
        call(
            "http://airflow.test/api/v2/dags/demo/dagRuns",
            headers={
                "Authorization": "Bearer token",
                "Accept": "application/json",
            },
            params={"limit": PAGE_SIZE, "offset": PAGE_SIZE},
            timeout=10.0,
        ),
    ]

    first_page.raise_for_status.assert_called_once_with()
    second_page.raise_for_status.assert_called_once_with()


@pytest.mark.parametrize(
    ("method_name", "collection_key", "expected_url", "args"),
    [
        (
            "get_task_instances",
            "task_instances",
            "http://airflow.test/api/v2/dags/demo/dagRuns/run_1/taskInstances",
            ("demo", "run_1"),
        ),
        (
            "get_dag_tasks",
            "tasks",
            "http://airflow.test/api/v2/dags/demo/tasks",
            ("demo",),
        ),
    ],
)
@patch("flowsense.infrastructure.airflow.client.httpx.get")
def test_paginated_endpoints_use_their_collection_key(
    mock_get: MagicMock,
    method_name: str,
    collection_key: str,
    expected_url: str,
    args: tuple[str, ...],
    client: AirflowClient,
) -> None:
    response = MagicMock()
    response.json.return_value = {
        collection_key: [{"id": "item_1"}],
        "total_entries": 1,
    }
    mock_get.return_value = response

    result = getattr(client, method_name)(*args)

    assert result[collection_key] == [{"id": "item_1"}]
    mock_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": "Bearer token",
            "Accept": "application/json",
        },
        params={"limit": PAGE_SIZE, "offset": 0},
        timeout=10.0,
    )
