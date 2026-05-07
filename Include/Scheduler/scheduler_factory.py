from __future__ import annotations

from enum import Enum
from typing import Any, List, Union

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


# фабрика планировщиков, по аналогии с фабрикой раннеров
class SchedulerFactory:
    @staticmethod
    def _object_id(obj: Any, default_prefix: str, index: int) -> str:
        if hasattr(obj, "get_id"):
            return str(obj.get_id())

        for attr_name in ("task_id", "runner_id", "id", "name"):
            if hasattr(obj, attr_name):
                value = getattr(obj, attr_name)

                if value is not None:
                    return str(value)

        return f"{default_prefix}-{index + 1}"

    @staticmethod
    def _make_task_map(tasks: List[ITask]) -> dict[str, ITask]:
        return {
            SchedulerFactory._object_id(task, "task", index): task
            for index, task in enumerate(tasks)
        }

    @staticmethod
    def _bind_scheduler_data(
        scheduler: IScheduler,
        tasks: List[ITask],
        runners: List[IRunner],
    ) -> IScheduler:
        # планировщики внутри используют self._tasks.values(),
        # поэтому _tasks обязан быть словарем task_id -> task
        tasks_by_id = SchedulerFactory._make_task_map(tasks)

        # ВАЖНО:
        # _runners должен быть списком, потому что run(...) ожидает List,
        # а SequentialDepsScheduler внутри берет runners[0].
        runners_list = list(runners)

        scheduler._tasks = tasks_by_id
        scheduler._runners = runners_list

        # дополнительные публичные поля для удобства отладки
        scheduler.tasks = tasks_by_id
        scheduler.runners = runners_list

        scheduler.task_map = tasks_by_id
        scheduler.task_list = list(tasks)
        scheduler.runner_list = runners_list

        # если у планировщика есть нормальный setter, тоже используем его
        if hasattr(scheduler, "set_runners"):
            scheduler.set_runners(runners_list)

        return scheduler

    @staticmethod
    def create_scheduler(
        kind: Union[SchedulerKind, str],
        tasks: List[ITask],
        runners: List[IRunner],
    ) -> IScheduler:
        if isinstance(kind, str):
            kind = SchedulerKind(kind)

        # проверка наличия исполнителей
        if not runners:
            raise ValueError("Runner list must not be empty")

        # выбор типа планировщика
        if kind == SchedulerKind.SEQUENTIAL:
            scheduler = SequentialDepsScheduler()
            return SchedulerFactory._bind_scheduler_data(
                scheduler=scheduler,
                tasks=tasks,
                runners=[runners[0]],
            )

        if kind == SchedulerKind.PARALLEL:
            scheduler = ParallelFIFODepScheduler()
            return SchedulerFactory._bind_scheduler_data(
                scheduler=scheduler,
                tasks=tasks,
                runners=runners,
            )

        if kind == SchedulerKind.DAG_PRIORITY:
            scheduler = DagUserPriorityScheduler()
            return SchedulerFactory._bind_scheduler_data(
                scheduler=scheduler,
                tasks=tasks,
                runners=runners,
            )

        if kind == SchedulerKind.DAG_CRITICAL:
            scheduler = DagCriticalPathScheduler()
            return SchedulerFactory._bind_scheduler_data(
                scheduler=scheduler,
                tasks=tasks,
                runners=runners,
            )

        raise ValueError(f"Unsupported scheduler kind: {kind}")

    @staticmethod
    def create(
        kind: Union[SchedulerKind, str],
        runners: List[IRunner],
        tasks: List[ITask],
    ) -> IScheduler:
        return SchedulerFactory.create_scheduler(
            kind=kind,
            tasks=tasks,
            runners=runners,
        )
