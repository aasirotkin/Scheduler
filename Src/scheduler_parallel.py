#!/usr/bin/env python3

from typing import Dict

from Src.base_scheduler import BaseScheduler
from Include.Scheduler.IFaces.task_spec import TaskSpec
from Include.Scheduler.IFaces.utils import  id_sort_key

#параллельный планировщик, в workers нет аргументов, 
class ParallelFIFODepScheduler(BaseScheduler):
    def get_name(self) -> str:
        return "parallel"

    def _make_ready_key(self, tasks: Dict[str, TaskSpec]):
        return lambda tid: (id_sort_key(tid),)
