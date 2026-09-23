from unittest.mock import MagicMock, call

import httpx
import pytest

from flowsense.infrastructure.airflow import (
    AirflowApiError,
    AirflowConfig,
    AirflowDagRunNotFoundError,
    AirflowDataError,
)
from flowsense.infrastructure.airflow.client import PAGE_SIZE, AirflowClient


@pytest.fixture
def http_client() -> MagicMock:
    return MagicMock(spec=httpx.Client)


@pytest.fixture
def client(http_client: MagicMock) -> AirflowClient:
    airflow_client = AirflowClient(
        config=AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
            max_retries=0,
        ),
        http_client=http_client,
    )

    airflow_client._token = "token"
    return airflow_client


def test_get_dag_runs_collects_all_pages(
    client: AirflowClient,
    http_client: MagicMock,
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

    http_client.request.side_effect = [first_page, second_page]

    result = client.get_dag_runs("demo")

    assert len(result["dag_runs"]) == PAGE_SIZE + 1
    assert result["total_entries"] == PAGE_SIZE + 1
    assert http_client.request.call_args_list == [
        call(
            method="GET",
            url="http://airflow.test/api/v2/dags/demo/dagRuns",
            headers={
                "Authorization": "Bearer token",
                "Accept": "application/json",
            },
            params={"limit": PAGE_SIZE, "offset": 0},
        ),
        call(
            method="GET",
            url="http://airflow.test/api/v2/dags/demo/dagRuns",
            headers={
                "Authorization": "Bearer token",
                "Accept": "application/json",
            },
            params={"limit": PAGE_SIZE, "offset": PAGE_SIZE},
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
def test_paginated_endpoints_use_their_collection_key(
    method_name: str,
    collection_key: str,
    expected_url: str,
    args: tuple[str, ...],
    client: AirflowClient,
    http_client: MagicMock,
) -> None:
    response = MagicMock()
    response.json.return_value = {
        collection_key: [{"id": "item_1"}],
        "total_entries": 1,
    }
    http_client.request.return_value = response

    result = getattr(client, method_name)(*args)

    assert result[collection_key] == [{"id": "item_1"}]
    http_client.request.assert_called_once_with(
        method="GET",
        url=expected_url,
        headers={
            "Authorization": "Bearer token",
            "Accept": "application/json",
        },
        params={"limit": PAGE_SIZE, "offset": 0},
    )


def test_does_not_close_injected_http_client(
    client: AirflowClient,
    http_client: MagicMock,
) -> None:
    client.close()

    http_client.close.assert_not_called()


def test_collect_task_runs_limits_to_latest_successful_dag_runs(
    client: AirflowClient,
) -> None:
    client.history_run_limit = 2
    client._get_recent_successful_dag_runs = MagicMock(
        return_value={
            "dag_runs": [
                {
                    "dag_run_id": "newest",
                    "state": "success",
                    "logical_date": "2026-01-04T00:00:00Z",
                },
                {
                    "dag_run_id": "failed",
                    "state": "failed",
                    "logical_date": "2026-01-05T00:00:00Z",
                },
                {
                    "dag_run_id": "oldest",
                    "state": "success",
                    "logical_date": "2026-01-01T00:00:00Z",
                },
                {
                    "dag_run_id": "middle",
                    "state": "success",
                    "logical_date": "2026-01-03T00:00:00Z",
                },
            ]
        }
    )
    client._get_successful_task_instances = MagicMock(
        return_value={
            "task_instances": [
                {
                    "task_id": f"task_demo_{dag_run_id}",
                    "dag_run_id": dag_run_id,
                    "state": "success",
                    "duration": 1.0,
                }
                for dag_run_id in ["middle", "newest"]
            ]
        }
    )

    task_runs = client.collect_task_runs("demo")

    assert [run.dag_run_id for run in task_runs] == ["middle", "newest"]
    client._get_successful_task_instances.assert_called_once_with(
        "demo", ["middle", "newest"]
    )


def test_collect_task_runs_ends_history_at_target_dag_run(
    client: AirflowClient,
) -> None:
    client.history_run_limit = 2
    client.target_dag_run_id = "middle"
    client.get_dag_runs = MagicMock(
        return_value={
            "dag_runs": [
                {
                    "dag_run_id": run_id,
                    "state": "success",
                    "logical_date": f"2026-01-0{day}T00:00:00Z",
                }
                for day, run_id in enumerate(
                    ["oldest", "middle", "newest"],
                    start=1,
                )
            ]
        }
    )
    client._get_successful_task_instances = MagicMock(
        return_value={
            "task_instances": [
                {
                    "task_id": f"task_demo_{dag_run_id}",
                    "dag_run_id": dag_run_id,
                    "state": "success",
                    "duration": 1.0,
                }
                for dag_run_id in ["oldest", "middle"]
            ]
        }
    )

    task_runs = client.collect_task_runs("demo")

    assert [run.dag_run_id for run in task_runs] == ["oldest", "middle"]


def test_collect_task_runs_rejects_missing_target_dag_run(
    client: AirflowClient,
) -> None:
    client.target_dag_run_id = "missing"
    client.get_dag_runs = MagicMock(return_value={"dag_runs": []})
    client.get_task_instances = MagicMock()

    with pytest.raises(AirflowDagRunNotFoundError, match="missing"):
        client.collect_task_runs("demo")

    client.get_task_instances.assert_not_called()


def test_rejects_history_run_limit_below_two(http_client: MagicMock) -> None:
    config = AirflowConfig(
        base_url="http://airflow.test",
        username="airflow",
        password="airflow",
    )

    with pytest.raises(ValueError, match="history_run_limit"):
        AirflowClient(config, history_run_limit=1, http_client=http_client)


def test_rejects_blank_target_dag_run_id(http_client: MagicMock) -> None:
    config = AirflowConfig(
        base_url="http://airflow.test",
        username="airflow",
        password="airflow",
    )

    with pytest.raises(ValueError, match="target_dag_run_id"):
        AirflowClient(config, target_dag_run_id=" ", http_client=http_client)


def test_wraps_http_status_errors_without_response_body(
    client: AirflowClient,
    http_client: MagicMock,
) -> None:
    request = httpx.Request(
        "GET",
        "http://airflow.test/api/v2/dags/demo/dagRuns",
    )
    http_client.request.return_value = httpx.Response(
        status_code=503,
        request=request,
        text="internal server details",
    )

    with pytest.raises(AirflowApiError) as exc_info:
        client.get_dag_runs("demo")

    assert exc_info.value.method == "GET"
    assert exc_info.value.endpoint == "/api/v2/dags/demo/dagRuns"
    assert exc_info.value.status_code == 503
    assert "internal server details" not in str(exc_info.value)


def test_wraps_invalid_paginated_json_as_airflow_data_error(
    client: AirflowClient,
    http_client: MagicMock,
) -> None:
    response = MagicMock()
    response.json.side_effect = ValueError("invalid JSON containing internals")
    http_client.request.return_value = response

    with pytest.raises(AirflowDataError, match="dag_runs") as exc_info:
        client.get_dag_runs("demo")

    assert "internals" not in str(exc_info.value)


def test_wraps_invalid_dag_run_schema_as_airflow_data_error(
    client: AirflowClient,
) -> None:
    client._get_recent_successful_dag_runs = MagicMock(
        return_value={"dag_runs": [{"state": "success"}]}
    )

    with pytest.raises(AirflowDataError, match="DAG run"):
        client.collect_task_runs("demo")


def test_airflow_3_filters_and_bounds_recent_successful_dag_runs(
    client: AirflowClient,
    http_client: MagicMock,
) -> None:
    client.history_run_limit = 25
    response = MagicMock()
    response.json.return_value = {"dag_runs": [], "total_entries": 1000}
    http_client.request.return_value = response

    client._get_recent_successful_dag_runs("demo")

    assert http_client.request.call_args.kwargs["params"] == {
        "states": ["success"],
        "order_by": "-run_after",
        "limit": 25,
        "offset": 0,
    }


def test_airflow_2_uses_batch_dag_run_filter(http_client: MagicMock) -> None:
    response = MagicMock()
    response.json.return_value = {"dag_runs": [], "total_entries": 0}
    http_client.request.return_value = response
    client = AirflowClient(
        AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
            api_version="v1",
            auth_mode="basic",
        ),
        history_run_limit=20,
        http_client=http_client,
    )

    client._get_recent_successful_dag_runs("demo")

    assert http_client.request.call_args.kwargs["json"] == {
        "dag_ids": ["demo"],
        "states": ["success"],
        "order_by": "-execution_date",
        "page_limit": 20,
        "page_offset": 0,
    }


def test_filtered_dag_run_query_falls_back_when_unsupported() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params.get("states"):
            return httpx.Response(422, request=request)
        return httpx.Response(
            200,
            request=request,
            json={"dag_runs": [], "total_entries": 0},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = AirflowClient(
            AirflowConfig(
                base_url="http://airflow.test",
                auth_mode="bearer",
                bearer_token="token",
                max_retries=0,
            ),
            http_client=http_client,
        )
        client._get_recent_successful_dag_runs("demo")

    assert len(requests) == 2
    assert requests[1].url.params.get("states") is None


def test_airflow_3_batches_successful_task_instances(
    client: AirflowClient,
    http_client: MagicMock,
) -> None:
    response = MagicMock()
    response.json.return_value = {"task_instances": [], "total_entries": 0}
    http_client.request.return_value = response

    client._get_successful_task_instances("demo", ["run_1", "run_2"])

    request = http_client.request.call_args.kwargs
    assert request["url"].endswith("/dags/demo/dagRuns/~/taskInstances")
    assert request["params"] == {
        "dag_run_ids": ["run_1", "run_2"],
        "states": ["success"],
        "limit": PAGE_SIZE,
        "offset": 0,
    }


def test_airflow_2_batches_successful_task_instances(http_client: MagicMock) -> None:
    response = MagicMock()
    response.json.return_value = {"task_instances": [], "total_entries": 0}
    http_client.request.return_value = response
    client = AirflowClient(
        AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
            api_version="v1",
            auth_mode="basic",
        ),
        http_client=http_client,
    )

    client._get_successful_task_instances("demo", ["run_1", "run_2"])

    request = http_client.request.call_args.kwargs
    assert request["url"].endswith("/dags/~/dagRuns/~/taskInstances/list")
    assert request["json"] == {
        "dag_ids": ["demo"],
        "dag_run_ids": ["run_1", "run_2"],
        "state": ["success"],
        "page_limit": PAGE_SIZE,
        "page_offset": 0,
    }


def test_batch_task_instances_fall_back_to_per_run_requests() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if "/~/taskInstances" in request.url.path:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200,
            request=request,
            json={
                "task_instances": [
                    {"task_id": "extract", "state": "success", "duration": 1.0}
                ],
                "total_entries": 1,
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = AirflowClient(
            AirflowConfig(
                base_url="http://airflow.test",
                auth_mode="bearer",
                bearer_token="token",
                max_retries=0,
            ),
            http_client=http_client,
        )
        result = client._get_successful_task_instances("demo", ["run_1", "run_2"])

    assert [item["dag_run_id"] for item in result["task_instances"]] == [
        "run_1",
        "run_2",
    ]
    assert len(requests) == 3
