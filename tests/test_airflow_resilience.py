from unittest.mock import MagicMock, call

import httpx
import pytest

from flowsense.infrastructure.airflow import AirflowApiError, AirflowConfig
from flowsense.infrastructure.airflow.client import AirflowClient


def _client(
    http_client: MagicMock,
    sleep: MagicMock,
    *,
    max_retries: int = 2,
    retry_backoff: float = 0.25,
    connect_timeout: float = 3.0,
    read_timeout: float = 7.0,
    auth_mode: str = "token",
) -> AirflowClient:
    client = AirflowClient(
        config=AirflowConfig(
            base_url="http://airflow.test",
            username="airflow",
            password="airflow",
            auth_mode=auth_mode,
            bearer_token="static-token" if auth_mode == "bearer" else None,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        ),
        http_client=http_client,
        sleep=sleep,
    )

    client._token = "token"
    return client


def _response(status_code: int, *, retry_after: str | None = None) -> httpx.Response:
    headers = {"Retry-After": retry_after} if retry_after is not None else None
    return httpx.Response(
        status_code,
        request=httpx.Request(
            "GET",
            "http://airflow.test/api/v2/dags/demo/dagRuns",
        ),
        headers=headers,
        json={"dag_runs": [], "total_entries": 0},
    )


def test_retries_transient_status_with_exponential_backoff() -> None:
    http_client = MagicMock(spec=httpx.Client)
    sleep = MagicMock()
    first_failure = _response(503)
    second_failure = _response(502)
    http_client.request.side_effect = [
        first_failure,
        second_failure,
        _response(200),
    ]
    client = _client(http_client, sleep)

    result = client.get_dag_runs("demo")

    assert result == {"dag_runs": [], "total_entries": 0}
    assert http_client.request.call_count == 3
    assert sleep.call_args_list == [call(0.25), call(0.5)]
    assert first_failure.is_closed
    assert second_failure.is_closed


def test_prefers_retry_after_header_over_backoff() -> None:
    http_client = MagicMock(spec=httpx.Client)
    sleep = MagicMock()
    http_client.request.side_effect = [
        _response(429, retry_after="3"),
        _response(200),
    ]
    client = _client(http_client, sleep)

    client.get_dag_runs("demo")

    sleep.assert_called_once_with(3.0)


def test_retries_transport_error() -> None:
    http_client = MagicMock(spec=httpx.Client)
    sleep = MagicMock()
    request = httpx.Request(
        "GET",
        "http://airflow.test/api/v2/dags/demo/dagRuns",
    )
    http_client.request.side_effect = [
        httpx.ConnectError("connection failed", request=request),
        _response(200),
    ]
    client = _client(http_client, sleep)

    client.get_dag_runs("demo")

    assert http_client.request.call_count == 2
    sleep.assert_called_once_with(0.25)


def test_does_not_retry_permanent_client_error() -> None:
    http_client = MagicMock(spec=httpx.Client)
    sleep = MagicMock()
    http_client.request.return_value = _response(401)
    client = _client(http_client, sleep, auth_mode="bearer")

    with pytest.raises(AirflowApiError) as exc_info:
        client.get_dag_runs("demo")

    assert exc_info.value.status_code == 401
    assert http_client.request.call_count == 1
    sleep.assert_not_called()


def test_reports_transient_error_after_retries_are_exhausted() -> None:
    http_client = MagicMock(spec=httpx.Client)
    sleep = MagicMock()
    http_client.request.side_effect = [_response(503), _response(503)]
    client = _client(http_client, sleep, max_retries=1)

    with pytest.raises(AirflowApiError) as exc_info:
        client.get_dag_runs("demo")

    assert exc_info.value.status_code == 503
    assert http_client.request.call_count == 2
    sleep.assert_called_once_with(0.25)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"connect_timeout": 0.0}, "connect_timeout"),
        ({"read_timeout": 0.0}, "read_timeout"),
        ({"max_retries": -1}, "max_retries"),
        ({"retry_backoff": -1.0}, "retry_backoff"),
    ],
)
def test_validates_resilience_configuration(
    overrides: dict[str, int | float],
    message: str,
) -> None:
    http_client = MagicMock(spec=httpx.Client)
    sleep = MagicMock()

    with pytest.raises(ValueError, match=message):
        _client(http_client, sleep, **overrides)
