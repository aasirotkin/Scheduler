#!/usr/bin/env python3

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Optional

from task_models import TaskResult, TaskSpec

#ИНТЕРФЕЙС ПЛАНИРОВЩИКА И РЕАЛИЗАЦИИ
#мини-интерфейс, у каждого есть имя и run(...)

class SchedulerBase(ABC):
    name: str = "base"

    @abstractmethod
    def run(self, task_py: str, tasks: Dict[str, TaskSpec], *, workers: int, jitter_pct: float, seed: Optional[int]) -> Dict[str, TaskResult]:
        pass
