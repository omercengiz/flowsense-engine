from __future__ import annotations

from typing import Self

import httpx

from flowsense.config import get_airflow_config
from flowsense.domain import TaskRun
from flowsense.infrastructure.airflow.dto import (
    AirflowDagRunDTO,
    AirflowTaskDTO,
    AirflowTaskInstanceDTO,
)
from flowsense.infrastructure.airflow.mapper import (
    map_dependencies,
    map_task_instance,
)

PAGE_SIZE = 100


class AirflowClient:
    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        http_client: httpx.Client | None = None,
    ):
        config = get_airflow_config()

        self.base_url = (base_url or config.base_url).rstrip("/")
        self.username = username or config.username
        self.password = password or config.password
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(timeout=10.0)
        self._token: str | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def _get_token(self) -> str:
        if self._token:
            return self._token

        response = self._http_client.post(
            f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self.password,
            },
        )
        response.raise_for_status()

        self._token = response.json()["access_token"]
        return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

    def _get_paginated(
        self,
        url: str,
        collection_key: str,
    ) -> dict:
        items: list[dict] = []
        offset = 0
        last_page: dict = {}

        while True:
            response = self._http_client.get(
                url,
                headers=self._headers(),
                params={"limit": PAGE_SIZE, "offset": offset},
            )
            response.raise_for_status()

            last_page = response.json()
            page_items = last_page[collection_key]
            items.extend(page_items)
            total_entries = last_page.get("total_entries")

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
            url=f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns",
            collection_key="dag_runs",
        )

    def get_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> dict:
        return self._get_paginated(
            url=(
                f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/"
                f"{dag_run_id}/taskInstances"
            ),
            collection_key="task_instances",
        )

    def collect_task_runs(self, dag_id: str) -> list[TaskRun]:
        response = self.get_dag_runs(dag_id)
        dag_runs = [
            AirflowDagRunDTO.model_validate(item) for item in response["dag_runs"]
        ]
        dag_runs.sort(
            key=lambda run: (
                (run.run_after or run.queued_at).timestamp()
                if run.run_after or run.queued_at
                else float("-inf")
            )
        )

        task_runs: list[TaskRun] = []

        for dag_run in dag_runs:
            if dag_run.state != "success":
                continue

            response = self.get_task_instances(
                dag_id=dag_id,
                dag_run_id=dag_run.dag_run_id,
            )
            task_instances = [
                AirflowTaskInstanceDTO.model_validate(item)
                for item in response["task_instances"]
            ]

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
            url=f"{self.base_url}/api/v2/dags/{dag_id}/tasks",
            collection_key="tasks",
        )

    def get_dag_dependencies(self, dag_id: str) -> dict[str, list[str]]:
        response = self.get_dag_tasks(dag_id)
        tasks = [AirflowTaskDTO.model_validate(item) for item in response["tasks"]]
        return map_dependencies(tasks)
