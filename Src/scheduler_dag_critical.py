#!/usr/bin/env python3

from typing import Dict

from base_scheduler import BaseScheduler
from runner_engine import compute_blevel
from task_spec import TaskSpec
from utils import id_sort_key


class DagCriticalPathScheduler(BaseScheduler):
    def get_name(self) -> str:
        return "dag_critical"

    def _make_ready_key(self, tasks: Dict[str, TaskSpec]):
        blevel = compute_blevel(tasks)
        return lambda tid: (-blevel[tid], -tasks[tid].priority, id_sort_key(tid))
