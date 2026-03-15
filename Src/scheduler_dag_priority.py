#!/usr/bin/env python3

from typing import Dict

from base_scheduler import BaseScheduler
from task_spec import TaskSpec
from utils import id_sort_key


class DagUserPriorityScheduler(BaseScheduler):
    def get_name(self) -> str:
        return "dag_priority"

    def _make_ready_key(self, tasks: Dict[str, TaskSpec]):
        return lambda tid: (-tasks[tid].priority, id_sort_key(tid))
