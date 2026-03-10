#!/usr/bin/env python3
#общая реалищация планировщика для остальных, без конкретики. Вынесли в одно место механику (хранение задач и результатоы, добавление и удаление задач, их запуск, вызов общего движка)

from abc import abstractmethod
from typing import Dict, List, Optional

from i_scheduler import IScheduler
from i_task import ITask
from task_result import TaskResult
from task_spec import TaskSpec
from runner_engine import run_with_deps


class BaseScheduler(IScheduler):
    def __init__(self, *, workers: int = 1, seed: Optional[int] = None, jitter_pct: float = 0.0):
        self._workers = workers
        self._seed = seed
        self._jitter_pct = jitter_pct
        self._tasks: Dict[str, ITask] = {}
        self._results: Dict[str, TaskResult] = {}

    def add_task(self, task: ITask) -> None:
        task_id = task.get_id()
        if task_id in self._tasks:
            raise ValueError(f"Task {task_id} already exists")
        self._tasks[task_id] = task

    def remove_task(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)
        self._results.pop(task_id, None)

    def list_tasks(self) -> List[str]:
        return sorted(self._tasks.keys())

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        return self._results.get(task_id)

    def get_results(self) -> Dict[str, TaskResult]:
        return dict(self._results)

    def run_all(self) -> Dict[str, TaskResult]:
        if not self._tasks:
            return {}

        tasks: Dict[str, TaskSpec] = {}
        task_py: Optional[str] = None

        for task in self._tasks.values():
            spec = task.get_spec()
            tasks[spec.id] = spec

            entrypoint = task.get_entrypoint()
            if task_py is None:
                task_py = entrypoint
            elif task_py != entrypoint:
                raise ValueError("All queued tasks must use the same task entrypoint")

        self._results = run_with_deps(
            task_py=task_py,
            tasks=tasks,
            workers=self._effective_workers(),
            ready_key=self._make_ready_key(tasks),
            jitter_pct=self._jitter_pct,
            seed=self._seed,
        )
        return dict(self._results)

    def _effective_workers(self) -> int:
        return self._workers

    @abstractmethod
    def _make_ready_key(self, tasks: Dict[str, TaskSpec]):
        ...
