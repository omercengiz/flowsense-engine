from datetime import UTC, datetime

from flowsense.infrastructure.airflow.dto import (
    AirflowTaskDTO,
    AirflowTaskInstanceDTO,
)
from flowsense.infrastructure.airflow.mapper import (
    map_dependencies,
    map_task_instance,
)


def test_maps_airflow_task_instance_to_domain_task_run() -> None:
    started_at = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    ended_at = datetime(2026, 9, 1, 10, 0, 3, tzinfo=UTC)
    dto = AirflowTaskInstanceDTO.model_validate(
        {
            "task_id": "transform",
            "state": "success",
            "start_date": started_at.isoformat(),
            "end_date": ended_at.isoformat(),
            "duration": 3.0,
            "try_number": 2,
            "map_index": 4,
            "airflow_only_field": "ignored",
        }
    )

    task_run = map_task_instance(
        dag_id="demo",
        dag_run_id="run_1",
        task=dto,
    )

    assert task_run.task_id == "transform"
    assert task_run.start_date == started_at
    assert task_run.end_date == ended_at
    assert task_run.try_number == 2
    assert task_run.map_index == 4


def test_maps_airflow_tasks_to_dependency_graph() -> None:
    tasks = [
        AirflowTaskDTO(
            task_id="extract",
            downstream_task_ids=["transform"],
        ),
        AirflowTaskDTO(
            task_id="transform",
        ),
    ]

    dependencies = map_dependencies(tasks)

    assert dependencies == {
        "extract": ["transform"],
        "transform": [],
    }
