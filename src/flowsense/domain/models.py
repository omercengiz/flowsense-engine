import math
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TaskRun:
    """Framework-independent representation of a completed task attempt."""

    dag_id: str
    dag_run_id: str
    task_id: str

    state: str | None = None

    start_date: datetime | None = None
    end_date: datetime | None = None

    duration: float | None = None
    try_number: int = 0

    map_index: int = -1

    def __post_init__(self) -> None:
        if self.duration is not None and (
            not math.isfinite(self.duration) or self.duration < 0
        ):
            raise ValueError("duration must be finite and non-negative")
