#!/usr/bin/env python3

from dataclasses import dataclass

#результаты, хранит информацию о статусе, длительности, результате
@dataclass
class TaskResult:
    task_id: str
    start_ts: float
    end_ts: float
    return_code: int
    status: str

    @property
    def duration_s(self) -> float:
        return self.end_ts - self.start_ts
