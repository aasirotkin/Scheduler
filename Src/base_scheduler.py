
from abc import abstractmethod
from typing import Dict, List, Optional

from Include.Scheduler.IFaces.i_scheduler import IScheduler
from Include.Scheduler.IFaces.i_task import ITask
from Include.Scheduler.IFaces.task_result import TaskResult

# общая база для всех планировщиков, здесь остаются параметры запуска, хранилище добавленных задач и результатов,базовые методы add/remove/list/get_result/get_results 
class BaseScheduler(IScheduler):
    def __init__(
        self,
        *,
        workers: int = 1,
        seed: Optional[int] = None,
        jitter_pct: float = 0.0,
    ):
# Сколько задач разрешено исполнять одновременно.
        self._workers = workers

        # параметры моделирования длительности задач
        self._seed = seed
        self._jitter_pct = jitter_pct

        # набор задач в планировщике
        self._tasks: Dict[str, ITask] = {}

        # результаты последнего запуска
        self._results: Dict[str, TaskResult] = {}

# добавляем  задачу в очередь 
    def add_task(self, task: ITask) -> None:
        task_id = task.get_id()
        if task_id in self._tasks:
            raise ValueError(f"Task {task_id} already exists")
        self._tasks[task_id] = task

#удаляем задачу из планировщика + очистка старого результата, если есть 
    def remove_task(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)
        self._results.pop(task_id, None)

# возвращает список ID задач, которые сейчас лежат в планировщике
    def list_tasks(self) -> List[str]:
        return sorted(self._tasks.keys())

# возвращает результат 1 задачи
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        return self._results.get(task_id)

# возвращает все результаты
    def get_results(self) -> Dict[str, TaskResult]:
        return dict(self._results)

    @abstractmethod
    def get_name(self) -> str:
        ...

# полный цикл планирования/ исполнения задач
    @abstractmethod
    def run_all(self) -> Dict[str, TaskResult]:
        ...
