#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from scheduler_interface import SchedulerBase
from task_models import TaskResult, TaskSpec, _id_sort_key
from runner_engine import compute_blevel, run_with_deps


@dataclass(frozen=True)
class DagCriticalPathData:
    blevel: Dict[str, float]


#DAG-граф + критический путь
class DagCriticalPathScheduler(SchedulerBase):
    name = "dag_critical"

    def run(self, task_py: str, tasks: Dict[str, TaskSpec], *, workers: int, jitter_pct: float, seed: Optional[int]) -> Dict[str, TaskResult]:
        data = DagCriticalPathData(blevel=compute_blevel(tasks))
        return run_with_deps(
            task_py, tasks,
            workers=workers,
            ready_key=lambda tid: (-data.blevel[tid], -tasks[tid].priority, _id_sort_key(tid)),
            jitter_pct=jitter_pct,
            seed=seed,
        )
