#!/usr/bin/env python3

from typing import Optional

from i_scheduler import IScheduler
from registry import create_scheduler_impl

#функция создания реализации класса для разных типов планировщика
def create_scheduler(
    kind: str,
    *,
    workers: int = 1,
    seed: Optional[int] = None,
    jitter_pct: float = 0.0,
) -> IScheduler:
#фабрика создания планировашика. определяет тип планировщика, количество воркеров, случайностей и вотклонений. возвращает интерфейс ISchedulor. Фабрика нужна, чтобы 1 - не торчали наружу конкретные классы, 2 - раьбота пользователя через публичный API, 3 - олин раз задавать параметры
    return create_scheduler_impl(
        kind=kind,
        workers=workers,
        seed=seed,
        jitter_pct=jitter_pct,
    )
