from datetime import datetime

from pydantic import BaseModel


class TaskRun(BaseModel):
    dag_id: str
    dag_run_id: str
    task_id: str

    state: str | None = None

    start_date: datetime | None = None
    end_date: datetime | None = None

    duration: float | None = None
    try_number: int = 0

    map_index: int = -1
