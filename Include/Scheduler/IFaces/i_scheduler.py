from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional

from Include.Scheduler.IFaces.i_task import ITask
from Include.Scheduler.IFaces.task_result import TaskResult


class SchedulerStatus(str, Enum):
    RUNNING = "running"
    FINISHED = "finished"


class IScheduler(ABC):
    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def get_status(self) -> SchedulerStatus:
        ...

    # добавить задачу в планировщик
    @abstractmethod
    def add_task(self, task: ITask) -> None:
        ...

    # удалить задачу по id
    @abstractmethod
    def remove_task(self, task_id: str) -> None:
        ...

    @abstractmethod
    def list_tasks(self) -> List[str]:
        ...

    @abstractmethod
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        ...

    @abstractmethod
    def get_results(self) -> Dict[str, TaskResult]:
        ...

    @abstractmethod
    def run_all(self) -> Dict[str, TaskResult]:
        ...
