#!/usr/bin/env python3

from __future__ import annotations

from typing import Dict, Optional

from scheduler_interface import SchedulerBase
from task_models import TaskResult, TaskSpec, _id_sort_key
from runner_engine import run_with_deps


#DAG-граф + приоритетность
class DagUserPriorityScheduler(SchedulerBase):
    name = "dag_priority"

    def run(self, task_py: str, tasks: Dict[str, TaskSpec], *, workers: int, jitter_pct: float, seed: Optional[int]) -> Dict[str, TaskResult]:
        return run_with_deps(
            task_py, tasks,
            workers=workers,
            ready_key=lambda tid: (-tasks[tid].priority, _id_sort_key(tid)),
            jitter_pct=jitter_pct,
            seed=seed,
        )
