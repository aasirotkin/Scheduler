#!/usr/bin/env python3

from typing import Dict, Optional

from base_scheduler import BaseScheduler
from task_spec import TaskSpec
from utils import id_sort_key

#Последовательный планировщик с учетом зависимостей, но workers = 1
class SequentialDepsScheduler(BaseScheduler):
    def get_name(self) -> str:
	return  "sequential"

    def _effective_workers(self) -> int:
        return 1

    def _make_ready_key(self, tasks: Dict[str, TaskSpec]):
        return lambda tid: (id_sort_key(tid),)
