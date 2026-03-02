#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Tuple

# Модель задачи + загрузка. Исправили планировщики (как именно они выбирают след. готовую задачу) и сделали один универсальный движок. Граничное условеи : если есть ссылка на несуществ. задачу - падаем, а не висим в рантайме 


@dataclass(frozen=True)
class TaskSpec:
    #Описание задачи, которое видит планировщик/оценочные параметры
    id: str
    mem_mb: float
    net_mbps: float
    cpu_percent: float
    duration: float
    priority: int = 0
    deps: Tuple[str, ...] = field(default_factory=tuple)


def _id_sort_key(x: str) -> Tuple[int, Any]:
    #сортировка id: '1','2','10' как числа по возрастанию
    return (0, int(x)) if x.isdigit() else (1, x)


#результаты, хранит информацию о статусе, длительности, результате


@dataclass
class TaskResult:
    task_id: str
    start_ts: float
    end_ts: float
    return_code: int
    status: str  # "ok" | "failed" | "skipped"

    @property
    def duration_s(self) -> float:
        return self.end_ts - self.start_ts
