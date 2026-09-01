from typing import Protocol

from flowsense.domain import TaskRun


class DAGDataSource(Protocol):
    def collect_task_runs(self, dag_id: str) -> list[TaskRun]: ...

    def get_dag_dependencies(self, dag_id: str) -> dict[str, list[str]]: ...
