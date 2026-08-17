from __future__ import annotations

import httpx

from flowsense.config import get_airflow_config
from flowsense.models import TaskRun


class AirflowClient:
    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        config = get_airflow_config()

        self.base_url = (base_url or config.base_url).rstrip("/")

        self.username = username or config.username
        self.password = password or config.password

        self._token: str | None = None

    def _get_token(self) -> str:
        if self._token:
            return self._token

        response = httpx.post(
            f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self.password,
            },
            timeout=10.0,
        )

        response.raise_for_status()

        data = response.json()
        self._token = data["access_token"]

        return self._token

    def _headers(self) -> dict[str, str]:
        token = self._get_token()

        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    def get_dag_runs(self, dag_id: str) -> dict:
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"

        response = httpx.get(
            url,
            headers=self._headers(),
            timeout=10.0,
        )

        response.raise_for_status()

        return response.json()

    def get_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> dict:
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"

        response = httpx.get(
            url,
            headers=self._headers(),
            timeout=10.0,
        )

        response.raise_for_status()

        return response.json()

    def collect_task_runs(
        self,
        dag_id: str,
    ) -> list[TaskRun]:
        runs = self.get_dag_runs(dag_id)

        task_runs: list[TaskRun] = []

        runs_data = sorted(
            runs["dag_runs"],
            key=lambda run: run.get("run_after") or run.get("queued_at") or "",
        )

        for run in runs_data:
            if run.get("state") != "success":
                continue

            run_id = run["dag_run_id"]

            tasks = self.get_task_instances(
                dag_id,
                run_id,
            )

            for task in tasks["task_instances"]:
                if task.get("state") != "success":
                    continue

                task_runs.append(
                    TaskRun(
                        dag_id=dag_id,
                        dag_run_id=run_id,
                        task_id=task["task_id"],
                        state=task.get("state"),
                        start_date=task.get("start_date"),
                        end_date=task.get("end_date"),
                        duration=task.get("duration"),
                        try_number=task.get("try_number", 0),
                        map_index=task.get("map_index", -1),
                    )
                )

        return task_runs

    def get_dag_tasks(
        self,
        dag_id: str,
    ) -> dict:
        url = f"{self.base_url}/api/v2/dags/{dag_id}/tasks"

        response = httpx.get(
            url,
            headers=self._headers(),
            timeout=10.0,
        )

        response.raise_for_status()

        return response.json()

    def get_dag_dependencies(
        self,
        dag_id: str,
    ) -> dict[str, list[str]]:
        data = self.get_dag_tasks(dag_id)

        dependencies: dict[str, list[str]] = {}

        for task in data["tasks"]:
            task_id = task["task_id"]

            dependencies[task_id] = task.get(
                "downstream_task_ids",
                [],
            )

        return dependencies
