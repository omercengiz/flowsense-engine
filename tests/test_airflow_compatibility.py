from unittest.mock import MagicMock, patch

import httpx
import pytest

from flowsense.config import AirflowConfig, get_airflow_config
from flowsense.infrastructure.airflow.client import AirflowClient
from flowsense.infrastructure.airflow.dto import (
    AirflowDagRunDTO,
    AirflowTaskDTO,
    AirflowTaskInstanceDTO,
)


def _client(
    http_client: MagicMock,
    *,
    api_version: str,
    auth_mode: str,
) -> AirflowClient:
    with patch(
        "flowsense.infrastructure.airflow.client.get_airflow_config",
        return_value=AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
            api_version=api_version,
            auth_mode=auth_mode,
        ),
    ):
        return AirflowClient(http_client=http_client)


def test_reads_api_compatibility_settings_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRFLOW_BASE_URL", "http://airflow.test")
    monkeypatch.setenv("AIRFLOW_USERNAME", "airflow")
    monkeypatch.setenv("AIRFLOW_PASSWORD", "airflow")
    monkeypatch.setenv("AIRFLOW_API_VERSION", "v1")
    monkeypatch.setenv("AIRFLOW_AUTH_MODE", "basic")

    config = get_airflow_config()

    assert config.api_version == "v1"
    assert config.auth_mode == "basic"


@pytest.mark.parametrize("api_version", ["v1", "v2"])
def test_uses_configured_stable_api_version(api_version: str) -> None:
    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.json.return_value = {"dag_runs": [], "total_entries": 0}
    http_client.request.return_value = response
    client = _client(http_client, api_version=api_version, auth_mode="token")
    client._token = "token"

    client.get_dag_runs("demo")

    assert http_client.request.call_args.kwargs["url"] == (
        f"http://airflow.test/api/{api_version}/dags/demo/dagRuns"
    )


def test_uses_basic_auth_for_airflow_2_api() -> None:
    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.json.return_value = {"dag_runs": [], "total_entries": 0}
    http_client.request.return_value = response
    client = _client(http_client, api_version="v1", auth_mode="basic")

    client.get_dag_runs("demo")

    request_kwargs = http_client.request.call_args.kwargs
    assert request_kwargs["headers"] == {"Accept": "application/json"}
    assert isinstance(request_kwargs["auth"], httpx.BasicAuth)


def test_uses_bearer_token_for_airflow_3_api() -> None:
    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.json.return_value = {"dag_runs": [], "total_entries": 0}
    http_client.request.return_value = response
    client = _client(http_client, api_version="v2", auth_mode="token")
    client._token = "token"

    client.get_dag_runs("demo")

    request_kwargs = http_client.request.call_args.kwargs
    assert request_kwargs["headers"] == {
        "Accept": "application/json",
        "Authorization": "Bearer token",
    }
    assert "auth" not in request_kwargs


@pytest.mark.parametrize(
    ("payload", "expected_timestamp"),
    [
        (
            {
                "dag_run_id": "airflow_2",
                "state": "success",
                "execution_date": "2026-01-01T10:00:00Z",
            },
            "2026-01-01T10:00:00+00:00",
        ),
        (
            {
                "dag_run_id": "airflow_2_modern",
                "state": "success",
                "logical_date": "2026-01-02T10:00:00Z",
            },
            "2026-01-02T10:00:00+00:00",
        ),
        (
            {
                "dag_run_id": "airflow_3",
                "state": "success",
                "run_after": "2026-01-03T10:00:00Z",
            },
            "2026-01-03T10:00:00+00:00",
        ),
    ],
)
def test_accepts_airflow_dag_run_timestamp_variants(
    payload: dict[str, object],
    expected_timestamp: str,
) -> None:
    dag_run = AirflowDagRunDTO.model_validate(payload)
    timestamp = dag_run.run_after or dag_run.logical_date or dag_run.queued_at

    assert timestamp is not None
    assert timestamp.isoformat() == expected_timestamp


@pytest.mark.parametrize(
    "payload",
    [
        {
            "task_id": "transform",
            "state": "success",
            "start_date": "2026-01-01T10:00:00Z",
            "end_date": "2026-01-01T10:00:03Z",
            "duration": 3.0,
            "try_number": 1,
        },
        {
            "id": "task-instance-id",
            "task_id": "transform",
            "state": "success",
            "start_date": "2026-01-01T10:00:00Z",
            "end_date": "2026-01-01T10:00:03Z",
            "duration": 3.0,
            "try_number": 1,
            "map_index": 2,
            "task_display_name": "Transform data",
            "dag_version": {"version_number": 4},
        },
    ],
)
def test_accepts_airflow_2_and_3_task_instance_payloads(
    payload: dict[str, object],
) -> None:
    task = AirflowTaskInstanceDTO.model_validate(payload)

    assert task.task_id == "transform"
    assert task.duration == 3.0


def test_accepts_extra_fields_in_airflow_task_payload() -> None:
    task = AirflowTaskDTO.model_validate(
        {
            "task_id": "extract",
            "downstream_task_ids": ["transform"],
            "operator_name": "PythonOperator",
            "is_mapped": False,
        }
    )

    assert task.downstream_task_ids == ["transform"]


@pytest.mark.parametrize(
    ("api_version", "auth_mode", "message"),
    [
        ("v3", "token", "api_version"),
        ("v2", "oauth", "auth_mode"),
    ],
)
def test_rejects_unsupported_client_configuration(
    api_version: str,
    auth_mode: str,
    message: str,
) -> None:
    http_client = MagicMock(spec=httpx.Client)

    with pytest.raises(ValueError, match=message):
        _client(
            http_client,
            api_version=api_version,
            auth_mode=auth_mode,
        )
