from dataclasses import dataclass
from enum import Enum


class TaskStatus(str, Enum):
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"

#результаты, хранит информацию о статусе, длительности, результате
@dataclass
class TaskResult:
    task_id: str
    start_ts: float
    end_ts: float
    return_code: int
    status: TaskStatus
    message: str = ""

    @property
    def duration_s(self) -> float:
        return self.end_ts - self.start_ts

