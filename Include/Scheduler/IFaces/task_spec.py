#!/usr/bin/env python3

from dataclasses import dataclass, field
from typing import Tuple

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
