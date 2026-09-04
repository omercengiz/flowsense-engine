from datetime import datetime

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class AirflowDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")


class AirflowDagRunDTO(AirflowDTO):
    dag_run_id: str
    state: str | None = None
    logical_date: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("logical_date", "execution_date"),
    )
    run_after: datetime | None = None
    queued_at: datetime | None = None


class AirflowTaskInstanceDTO(AirflowDTO):
    task_id: str
    state: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration: float | None = None
    try_number: int = 0
    map_index: int = -1


class AirflowTaskDTO(AirflowDTO):
    task_id: str
    downstream_task_ids: list[str] = Field(default_factory=list)
