from unittest.mock import MagicMock

import httpx
import pytest

from flowsense.infrastructure.airflow import (
    AirflowApiError,
    AirflowClient,
    AirflowConfig,
    AirflowRequestAuth,
    BasicAuthProvider,
    BearerTokenAuthProvider,
    HeaderAuthProvider,
)


def test_basic_auth_provider_returns_httpx_auth() -> None:
    context = BasicAuthProvider("airflow", "secret").request_auth()

    assert context.headers == {}
    assert isinstance(context.auth, httpx.BasicAuth)


def test_bearer_provider_resolves_rotating_token_per_request() -> None:
    tokens = iter(["first", "second"])
    provider = BearerTokenAuthProvider(tokens.__next__)

    assert provider.request_auth().headers == {"Authorization": "Bearer first"}
    assert provider.request_auth().headers == {"Authorization": "Bearer second"}


def test_header_provider_copies_validated_headers() -> None:
    headers = {"X-Forwarded-User": "service-account"}
    provider = HeaderAuthProvider(headers)
    headers["X-Forwarded-User"] = "changed"

    assert provider.request_auth().headers == {"X-Forwarded-User": "service-account"}


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"": "value"},
        {"X-Auth": ""},
        {"X-Auth\nInjected": "value"},
        {"X-Auth": "value\r\nInjected"},
    ],
)
def test_header_provider_rejects_unsafe_headers(headers: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="authentication header"):
        HeaderAuthProvider(headers)


def test_client_accepts_custom_auth_provider_without_credentials() -> None:
    http_client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.json.return_value = {"dag_runs": [], "total_entries": 0}
    http_client.request.return_value = response
    provider = MagicMock()
    provider.request_auth.return_value = AirflowRequestAuth(
        headers={"X-Company-Identity": "flowsense"}
    )
    client = AirflowClient(
        AirflowConfig(
            base_url="http://airflow.test",
            auth_mode="external",
        ),
        http_client=http_client,
        auth_provider=provider,
    )

    client.get_dag_runs("demo")

    provider.request_auth.assert_called_once_with()
    assert http_client.request.call_args.kwargs["headers"] == {
        "Accept": "application/json",
        "X-Company-Identity": "flowsense",
    }


def test_static_bearer_mode_does_not_call_login_endpoint() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"dag_runs": [], "total_entries": 0},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = AirflowClient(
            AirflowConfig(
                base_url="http://airflow.test",
                auth_mode="bearer",
                bearer_token="static-token",
            ),
            http_client=http_client,
        )
        client.get_dag_runs("demo")

    assert [request.url.path for request in requests] == ["/api/v2/dags/demo/dagRuns"]
    assert requests[0].headers["Authorization"] == "Bearer static-token"


def test_login_token_is_refreshed_once_after_unauthorized_response() -> None:
    requests: list[httpx.Request] = []
    issued_tokens = iter(["expired-token", "fresh-token"])

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"access_token": next(issued_tokens)})
        if request.headers["Authorization"] == "Bearer expired-token":
            return httpx.Response(401)
        return httpx.Response(200, json={"dag_runs": [], "total_entries": 0})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = AirflowClient(
            AirflowConfig(
                base_url="http://airflow.test",
                username="airflow",
                password="secret",
                auth_mode="token",
                max_retries=0,
            ),
            http_client=http_client,
        )
        client.get_dag_runs("demo")

    assert [request.url.path for request in requests] == [
        "/auth/token",
        "/api/v2/dags/demo/dagRuns",
        "/auth/token",
        "/api/v2/dags/demo/dagRuns",
    ]
    assert requests[-1].headers["Authorization"] == "Bearer fresh-token"


def test_login_token_refresh_retries_api_request_only_once() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/auth/token":
            token_number = sum(item.url.path == "/auth/token" for item in requests)
            return httpx.Response(200, json={"access_token": f"token-{token_number}"})
        return httpx.Response(401)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = AirflowClient(
            AirflowConfig(
                base_url="http://airflow.test",
                username="airflow",
                password="secret",
                auth_mode="token",
                max_retries=0,
            ),
            http_client=http_client,
        )
        with pytest.raises(AirflowApiError, match="status 401"):
            client.get_dag_runs("demo")

    assert [request.url.path for request in requests].count("/auth/token") == 2
    assert [request.url.path for request in requests].count(
        "/api/v2/dags/demo/dagRuns"
    ) == 2


def test_config_repr_does_not_expose_secrets() -> None:
    config = AirflowConfig(
        base_url="http://airflow.test",
        password="password-secret",
        bearer_token="token-secret",
    )

    assert "password-secret" not in repr(config)
    assert "token-secret" not in repr(config)
