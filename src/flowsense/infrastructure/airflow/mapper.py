from flowsense.domain import TaskRun
from flowsense.infrastructure.airflow.dto import (
    AirflowTaskDTO,
    AirflowTaskInstanceDTO,
)


def map_task_instance(
    dag_id: str,
    dag_run_id: str,
    task: AirflowTaskInstanceDTO,
) -> TaskRun:
    return TaskRun(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task.task_id,
        state=task.state,
        start_date=task.start_date,
        end_date=task.end_date,
        duration=task.duration,
        try_number=task.try_number,
        map_index=task.map_index,
    )


def map_dependencies(tasks: list[AirflowTaskDTO]) -> dict[str, list[str]]:
    return {task.task_id: list(task.downstream_task_ids) for task in tasks}
