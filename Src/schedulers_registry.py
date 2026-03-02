#!/usr/bin/env python3

from __future__ import annotations

from typing import Dict

from scheduler_interface import SchedulerBase
from scheduler_dag_critical import DagCriticalPathScheduler
from scheduler_dag_priority import DagUserPriorityScheduler
from scheduler_parallel import ParallelFIFODepScheduler
from scheduler_sequential import SequentialDepsScheduler


SCHEDULERS: Dict[str, SchedulerBase] = {
    "sequential": SequentialDepsScheduler(),
    "parallel": ParallelFIFODepScheduler(),
    "dag_priority": DagUserPriorityScheduler(),
    "dag_critical": DagCriticalPathScheduler(),
}
