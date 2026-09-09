from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal, Self

import httpx
from pydantic import ValidationError

from flowsense.config import get_airflow_config
from flowsense.domain import ConfigurationError, TaskRun
from flowsense.infrastructure.airflow.dto import (
    AirflowDagRunDTO,
    AirflowTaskDTO,
    AirflowTaskInstanceDTO,
)
from flowsense.infrastructure.airflow.exceptions import (
    AirflowApiError,
    AirflowDataError,
)
from flowsense.infrastructure.airflow.mapper import (
    map_dependencies,
    map_task_instance,
)

PAGE_SIZE = 100
AirflowApiVersion = Literal["v1", "v2"]
AirflowAuthMode = Literal["basic", "token"]
RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})


class AirflowClient:
    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        api_version: AirflowApiVersion | None = None,
        auth_mode: AirflowAuthMode | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        max_retries: int | None = None,
        retry_backoff: float | None = None,
        history_run_limit: int | None = None,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] | None = None,
    ):
        config = get_airflow_config()

        self.base_url = (base_url or config.base_url).rstrip("/")
        self.username = username or config.username
        self.password = password or config.password
        self.api_version = api_version or config.api_version
        self.auth_mode = auth_mode or config.auth_mode
        self.connect_timeout = (
            connect_timeout if connect_timeout is not None else config.connect_timeout
        )
        self.read_timeout = (
            read_timeout if read_timeout is not None else config.read_timeout
        )
        self.max_retries = (
            max_retries if max_retries is not None else config.max_retries
        )
        self.retry_backoff = (
            retry_backoff if retry_backoff is not None else config.retry_backoff
        )
        self.history_run_limit = (
            history_run_limit
            if history_run_limit is not None
            else config.history_run_limit
        )

        if self.api_version not in {"v1", "v2"}:
            raise ConfigurationError("api_version must be 'v1' or 'v2'")

        if self.auth_mode not in {"basic", "token"}:
            raise ConfigurationError("auth_mode must be 'basic' or 'token'")
        if self.connect_timeout <= 0:
            raise ConfigurationError("connect_timeout must be positive")
        if self.read_timeout <= 0:
            raise ConfigurationError("read_timeout must be positive")
        if self.max_retries < 0:
            raise ConfigurationError("max_retries must be non-negative")
        if self.retry_backoff < 0:
            raise ConfigurationError("retry_backoff must be non-negative")
        if self.history_run_limit < 2:
            raise ConfigurationError("history_run_limit must be at least 2")

        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(
            timeout=httpx.Timeout(
                connect=self.connect_timeout,
                read=self.read_timeout,
                write=self.read_timeout,
                pool=self.connect_timeout,
            )
        )
        self._sleep = sleep or time.sleep
        self._token: str | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: object,
    ) -> httpx.Response:
        request_kwargs: dict[str, Any] = dict(kwargs)

        for attempt in range(self.max_retries + 1):
            try:
                response = self._http_client.request(
                    method=method,
                    url=url,
                    **request_kwargs,
                )
            except httpx.RequestError as exc:
                if attempt < self.max_retries:
                    self._sleep(self._backoff_delay(attempt))
                    continue

                raise AirflowApiError(
                    method=method,
                    endpoint=exc.request.url.path,
                ) from exc

            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and attempt < self.max_retries
            ):
                delay = self._retry_delay(response, attempt)
                response.close()
                self._sleep(delay)
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AirflowApiError(
                    method=method,
                    endpoint=exc.request.url.path,
                    status_code=exc.response.status_code,
                ) from exc

            return response

        raise RuntimeError("Airflow request retry loop exited unexpectedly")

    def _backoff_delay(self, attempt: int) -> float:
        return self.retry_backoff * (2**attempt)

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            return self._backoff_delay(attempt)

        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=UTC)
                return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return self._backoff_delay(attempt)

    def _get_token(self) -> str:
        if self._token:
            return self._token

        response = self._request(
            method="POST",
            url=f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self.password,
            },
        )

        payload = self._response_json(response, "authentication response")
        token = payload.get("access_token")
        if not isinstance(token, str) or not token:
            raise AirflowDataError("authentication response")
        self._token = token
        return token

    @staticmethod
    def _response_json(
        response: httpx.Response,
        resource: str,
    ) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise AirflowDataError(resource) from exc

        if not isinstance(payload, dict):
            raise AirflowDataError(resource)

        return payload

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}

        if self.auth_mode == "token":
            headers["Authorization"] = f"Bearer {self._get_token()}"

        return headers

    def _authentication(self) -> httpx.BasicAuth | None:
        if self.auth_mode == "basic":
            return httpx.BasicAuth(self.username, self.password)

        return None

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}/api/{self.api_version}{path}"

    def _get_paginated(
        self,
        url: str,
        collection_key: str,
    ) -> dict:
        items: list[dict] = []
        offset = 0
        last_page: dict = {}

        while True:
            request_kwargs: dict[str, object] = {
                "headers": self._headers(),
                "params": {"limit": PAGE_SIZE, "offset": offset},
            }
            authentication = self._authentication()
            if authentication is not None:
                request_kwargs["auth"] = authentication

            response = self._request(
                method="GET",
                url=url,
                **request_kwargs,
            )

            last_page = self._response_json(response, collection_key)
            page_items = last_page.get(collection_key)
            if not isinstance(page_items, list):
                raise AirflowDataError(collection_key)
            items.extend(page_items)
            total_entries = last_page.get("total_entries")
            if total_entries is not None and not isinstance(total_entries, int):
                raise AirflowDataError(collection_key)

            if not page_items:
                break

            if total_entries is not None and len(items) >= total_entries:
                break

            if total_entries is None and len(page_items) < PAGE_SIZE:
                break

            offset += len(page_items)

        return {
            **last_page,
            collection_key: items,
            "total_entries": last_page.get("total_entries", len(items)),
        }

    def get_dag_runs(self, dag_id: str) -> dict:
        return self._get_paginated(
            url=self._api_url(f"/dags/{dag_id}/dagRuns"),
            collection_key="dag_runs",
        )

    def get_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> dict:
        return self._get_paginated(
            url=self._api_url(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"),
            collection_key="task_instances",
        )

    def collect_task_runs(self, dag_id: str) -> list[TaskRun]:
        response = self.get_dag_runs(dag_id)
        try:
            dag_runs = [
                AirflowDagRunDTO.model_validate(item) for item in response["dag_runs"]
            ]
        except ValidationError as exc:
            raise AirflowDataError("DAG run") from exc

        def run_timestamp(run: AirflowDagRunDTO) -> float:
            timestamp = run.run_after or run.logical_date or run.queued_at
            return timestamp.timestamp() if timestamp is not None else float("-inf")

        successful_dag_runs = [run for run in dag_runs if run.state == "success"]
        successful_dag_runs.sort(key=run_timestamp)
        selected_dag_runs = successful_dag_runs[-self.history_run_limit :]

        task_runs: list[TaskRun] = []

        for dag_run in selected_dag_runs:
            response = self.get_task_instances(
                dag_id=dag_id,
                dag_run_id=dag_run.dag_run_id,
            )
            try:
                task_instances = [
                    AirflowTaskInstanceDTO.model_validate(item)
                    for item in response["task_instances"]
                ]
            except ValidationError as exc:
                raise AirflowDataError("task instance") from exc

            task_runs.extend(
                map_task_instance(
                    dag_id=dag_id,
                    dag_run_id=dag_run.dag_run_id,
                    task=task,
                )
                for task in task_instances
                if task.state == "success"
            )

        return task_runs

    def get_dag_tasks(self, dag_id: str) -> dict:
        return self._get_paginated(
            url=self._api_url(f"/dags/{dag_id}/tasks"),
            collection_key="tasks",
        )

    def get_dag_dependencies(self, dag_id: str) -> dict[str, list[str]]:
        response = self.get_dag_tasks(dag_id)
        try:
            tasks = [AirflowTaskDTO.model_validate(item) for item in response["tasks"]]
        except ValidationError as exc:
            raise AirflowDataError("DAG task") from exc
        return map_dependencies(tasks)
