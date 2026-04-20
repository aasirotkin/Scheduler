#!/usr/bin/env python3

from typing import Dict, Optional, Type

from Src.base_scheduler import BaseScheduler
from Src.scheduler_dag_critical import DagCriticalPathScheduler
from Src.scheduler_dag_priority import DagUserPriorityScheduler
from Src.scheduler_parallel import ParallelFIFODepScheduler
from Src.scheduler_sequential import SequentialDepsScheduler


_REGISTRY: Dict[str, Type[BaseScheduler]] = {
    "sequential": SequentialDepsScheduler,
    "parallel": ParallelFIFODepScheduler,
    "dag_priority": DagUserPriorityScheduler,
    "dag_critical": DagCriticalPathScheduler,
}


def create_scheduler_impl(kind: str, *, workers: int, seed: Optional[int], jitter_pct: float) -> BaseScheduler:
    if kind not in _REGISTRY:
        raise ValueError(f"Unknown scheduler kind: {kind}. Allowed: {sorted(_REGISTRY.keys())}")

    cls = _REGISTRY[kind]
    return cls(workers=workers, seed=seed, jitter_pct=jitter_pct)
