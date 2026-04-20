#!/usr/bin/env python3

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from i_task import ITask
from task_result import TaskResult


@dataclass(frozen=True)
class RunnerResources:
    cpu_percent: float
    mem_mb: float
    net_mbps: float

    def fits(self, other: "RunnerResources") -> bool:
        return (
            self.cpu_percent >= other.cpu_percent
            and self.mem_mb >= other.mem_mb
            and self.net_mbps >= other.net_mbps
        )

    def add(self, other: "RunnerResources") -> "RunnerResources":
        return RunnerResources(
            cpu_percent=self.cpu_percent + other.cpu_percent,
            mem_mb=self.mem_mb + other.mem_mb,
            net_mbps=self.net_mbps + other.net_mbps,
        )

    def sub(self, other: "RunnerResources") -> "RunnerResources":
        return RunnerResources(
            cpu_percent=max(0.0, self.cpu_percent - other.cpu_percent),
            mem_mb=max(0.0, self.mem_mb - other.mem_mb),
            net_mbps=max(0.0, self.net_mbps - other.net_mbps),
        )

# интерфейс исполнителя, отвечает за запуск/отказ от задачи, как ее запукать и как вернуть результаты
class IRunner(ABC):

    @abstractmethod
    def get_id(self) -> str:
        ...

    @abstractmethod
    def total_resources(self) -> RunnerResources:
        ...

    @abstractmethod
    def used_resources(self) -> RunnerResources:
        ...

    @abstractmethod
    def free_resources(self) -> RunnerResources:
        ...

    @abstractmethod
    def power(self) -> float:
        ...

    @abstractmethod
    def power_left(self) -> float:
        ...

    @abstractmethod
    def can_run(self, task: ITask) -> bool:
        ...

    @abstractmethod
    def active_task_ids(self) -> List[str]:
        ...

    @abstractmethod
    def active_task_count(self) -> int:
        ...

    @abstractmethod
    def has_running_tasks(self) -> bool:
        ...

    @abstractmethod
    def submit(self, task: ITask) -> None:
        ...
#возврат одного завершившегося результата, если он уже есть
    @abstractmethod
    def poll_result(self) -> Optional[TaskResult]:
        ...

    @abstractmethod
    def poll_results(self) -> List[TaskResult]:
        ...

    @abstractmethod
    def shutdown(self) -> None:
        ...
