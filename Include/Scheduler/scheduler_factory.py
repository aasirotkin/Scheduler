from __future__ import annotations

from enum import Enum
from typing import List, Union

from Include.Scheduler.IFaces.i_runner import IRunner
from Include.Scheduler.IFaces.i_scheduler import IScheduler
from Include.Scheduler.IFaces.i_task import ITask

from Src.scheduler_dag_critical import DagCriticalPathScheduler
from Src.scheduler_dag_priority import DagUserPriorityScheduler
from Src.scheduler_parallel import ParallelFIFODepScheduler
from Src.scheduler_sequential import SequentialDepsScheduler


class SchedulerKind(str, Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    DAG_PRIORITY = "dag_priority"
    DAG_CRITICAL = "dag_critical"


# фабрика создания планировщика: принимает тип планировщика, список задач и список исполнителей
# список runners нужен как конфигурация, по нему берём число workers
# а сами задачи уже докидываем в scheduler через add_task

# фабрика планировщиков, по аналогии с фабрикой раннеров
class SchedulerFactory:
    @staticmethod

    def create_scheduler(
        kind: Union[SchedulerKind, str],
        tasks: List[ITask],
        runners: List[IRunner],
    ) -> IScheduler:
        if isinstance(kind, str):
            kind = SchedulerKind(kind)
#проверка наличия исполнителей
        if not runners:
            raise ValueError("Runner list must not be empty")

#количество исполнителей из списка раннеров
        workers = len(runners)

#прочек на тип планировшика
        if kind == SchedulerKind.SEQUENTIAL:
            scheduler: IScheduler = SequentialDepsScheduler(workers=1)
        elif kind == SchedulerKind.PARALLEL:
            scheduler = ParallelFIFODepScheduler(workers=workers)
        elif kind == SchedulerKind.DAG_PRIORITY:
            scheduler = DagUserPriorityScheduler(workers=workers)
        elif kind == SchedulerKind.DAG_CRITICAL:
            scheduler = DagCriticalPathScheduler(workers=workers)
        else:
            raise ValueError(f"Unsupported scheduler kind: {kind}")

    
        return scheduler
