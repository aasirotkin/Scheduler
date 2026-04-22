from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from Include.Scheduler.IFaces.i_task import ITask
from Include.Scheduler.IFaces.task_result import TaskResult


@dataclass(frozen=True)
class RunnerResources:
    cpu_percent: float
    mem_mb: float
    net_mbps: float

    # сложение ресурсов
    def add(self, other: "RunnerResources") -> "RunnerResources":
        return RunnerResources(
            cpu_percent=self.cpu_percent + other.cpu_percent,
            mem_mb=self.mem_mb + other.mem_mb,
            net_mbps=self.net_mbps + other.net_mbps,
        )

    # вычитание ресурсов
    def sub(self, other: "RunnerResources") -> "RunnerResources":
        return RunnerResources(
            cpu_percent=max(0.0, self.cpu_percent - other.cpu_percent),
            mem_mb=max(0.0, self.mem_mb - other.mem_mb),
            net_mbps=max(0.0, self.net_mbps - other.net_mbps),
        )

    # влезает ли одна тройка ресурсов в другую
    def fits(self, other: "RunnerResources") -> bool:
        return (
            self.cpu_percent >= other.cpu_percent
            and self.mem_mb >= other.mem_mb
            and self.net_mbps >= other.net_mbps
        )


# интерфейс исполнителя: хранит и считает свои ресурсы, принимает задачу к исполнению,
# позволяет получить готовые результаты и завершить активные задачи
class IRunner(ABC):
    @abstractmethod
    def get_id(self) -> str:
        ...

    # полные ресурсы (cpu_percent, mem_mb, net_mbps)
    @abstractmethod
    def total_resources(self) -> RunnerResources:
        ...

    # занятые ресурсы (cpu_percent, mem_mb, net_mbps)
    @abstractmethod
    def used_resources(self) -> RunnerResources:
        ...

    # свободные ресурсы
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
    def active_task_ids(self) -> List[str]:
        ...

    @abstractmethod
    def active_task_count(self) -> int:
        ...

    @abstractmethod
    def has_running_tasks(self) -> bool:
        ...

    # можно ли сейчас запустить эту задачу
    @abstractmethod
    def can_run(self, task: ITask) -> bool:
        ...

    # запуск задачи, если хватает ресурсов
    @abstractmethod
    def submit(self, task: ITask) -> None:
        ...

    @abstractmethod
    def poll_result(self) -> Optional[TaskResult]:
        ...

    @abstractmethod
    def poll_results(self) -> List[TaskResult]:
        ...

    @abstractmethod
    def shutdown(self) -> None:
        ...
