from __future__ import annotations

from typing import Any, List

from Include.Scheduler.IFaces.i_runner import IRunner
from Include.Scheduler.IFaces.i_scheduler import IScheduler
from Include.Scheduler.IFaces.i_task import ITask


class SchedulerFactoryHelper:
    @staticmethod
    def object_id(obj: Any, default_prefix: str, index: int) -> str:
        if hasattr(obj, "get_id"):
            return str(obj.get_id())

        for attr_name in ("task_id", "runner_id", "id", "name"):
            if hasattr(obj, attr_name):
                value = getattr(obj, attr_name)

                if value is not None:
                    return str(value)

        return f"{default_prefix}-{index + 1}"

    @staticmethod
    def make_task_map(tasks: List[ITask]) -> dict[str, ITask]:
        return {
            SchedulerFactoryHelper.object_id(task, "task", index): task
            for index, task in enumerate(tasks)
        }

    @staticmethod
    def bind_scheduler_data(
        scheduler: IScheduler,
        tasks: List[ITask],
        runners: List[IRunner],
    ) -> IScheduler:
        # планировщики внутри используют self._tasks.values(),
        # поэтому _tasks обязан быть словарем task_id -> task
        tasks_by_id = SchedulerFactoryHelper.make_task_map(tasks)

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
