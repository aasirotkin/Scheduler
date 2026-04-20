#!/usr/bin/env python3

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from Include.Scheduler.IFaces.task_result import TaskResult


# интерфейс исполнителя: хранит и считает свои ресурсы,принимает задачу к исполнению, позволяет получить результат по id, если он уже готов
class IRunner(ABC):

    @abstractmethod
    def get_id(self) -> str:
        ...

# полные ресурсы   (cpu_percent, mem_mb, net_mbps)
    @abstractmethod
    def total_resources(self) -> Tuple[float, float, float]:
        ...

# занятые ресурсы   (cpu_percent, mem_mb, net_mbps)
    @abstractmethod
    def used_resources(self) -> Tuple[float, float, float]:
        ...

    def add_resources(
        self,
        left: Tuple[float, float, float],
        right: Tuple[float, float, float],
    ) -> Tuple[float, float, float]:
        return (
            left[0] + right[0],
            left[1] + right[1],
            left[2] + right[2],
        )

    def sub_resources(
        self,
        left: Tuple[float, float, float],
        right: Tuple[float, float, float],
    ) -> Tuple[float, float, float]:
        return (
            max(0.0, left[0] - right[0]),
            max(0.0, left[1] - right[1]),
            max(0.0, left[2] - right[2]),
        )

# влезает ли     
    def fits_resources(
        self,
        available: Tuple[float, float, float],
        required: Tuple[float, float, float],
    ) -> bool:
        return (
            available[0] >= required[0]
            and available[1] >= required[1]
            and available[2] >= required[2]
        )

# свободные     
    def free_resources(self) -> Tuple[float, float, float]:
        return self.sub_resources(
            self.total_resources(),
            self.used_resources(),
        )

    @abstractmethod
    def power(self) -> float:
        ...

    @abstractmethod
    def power_left(self) -> float:
        ...

    def can_run(
        self,
        cpu_percent: float,
        mem_mb: float,
        net_mbps: float,
    ) -> bool:
        required = (cpu_percent, mem_mb, net_mbps)
        return self.fits_resources(self.free_resources(), required)

    @abstractmethod
    def active_task_ids(self) -> List[str]:
        ...

    @abstractmethod
    def active_task_count(self) -> int:
        ...

    @abstractmethod
    def has_running_tasks(self) -> bool:
        ...

# запуск скрипта, если хватает ресурсов. возвращает True, если выполняет задачу
    @abstractmethod
    def submit(
        self,
        task_id: str,
        script: str,
        script_kind: str,
        cpu_percent: float,
        mem_mb: float,
        net_mbps: float,
    ) -> bool:
        ...

    @abstractmethod
    def get_result_if_ready(self, task_id: str) -> Optional[TaskResult]:
        ...

    @abstractmethod
    def poll_results(self) -> List[TaskResult]:
        ...
