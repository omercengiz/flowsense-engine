from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field

Duration = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class TaskRun(BaseModel):
    dag_id: str
    dag_run_id: str
    task_id: str

    state: str | None = None

    start_date: datetime | None = None
    end_date: datetime | None = None

    duration: Duration | None = None
    try_number: int = 0

    map_index: int = -1
