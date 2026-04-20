#!/usr/bin/env python3

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
def create_scheduler(
    kind: Union[SchedulerKind, str],
    tasks: List[ITask],
    runners: List[IRunner],
) -> IScheduler:
    if isinstance(kind, str):
        kind = SchedulerKind(kind)

    if not runners:
        raise ValueError("Runner list must not be empty")

    if kind == SchedulerKind.SEQUENTIAL:
        return SequentialDepsScheduler(tasks=tasks, runners=runners)

    if kind == SchedulerKind.PARALLEL:
        return ParallelFIFODepScheduler(tasks=tasks, runners=runners)

    if kind == SchedulerKind.DAG_PRIORITY:
        return DagUserPriorityScheduler(tasks=tasks, runners=runners)

    if kind == SchedulerKind.DAG_CRITICAL:
        return DagCriticalPathScheduler(tasks=tasks, runners=runners)

    raise ValueError(f"Unsupported scheduler kind: {kind}")
