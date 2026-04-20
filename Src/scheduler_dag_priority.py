#!/usr/bin/env python3

from typing import Dict

from Src.base_scheduler import BaseScheduler
from Include.Scheduler.IFaces.task_spec import TaskSpec
from Include.Scheduler.IFaces.utils import id_sort_key


class DagUserPriorityScheduler(BaseScheduler):
    def get_name(self) -> str:
        return "dag_priority"

    def _make_ready_key(self, tasks: Dict[str, TaskSpec]):
        return lambda tid: (-tasks[tid].priority, id_sort_key(tid))
