#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from i_task import ITask
from task_result import TaskResult


class IScheduler(ABC):
    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def add_task(self, task: ITask) -> None:
        ...

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
