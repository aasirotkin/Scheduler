import heapq
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

from Src.base_scheduler import BaseScheduler
from Include.Scheduler.IFaces.task_result import TaskResult
from Include.Scheduler.IFaces.task_spec import TaskSpec
from Include.Scheduler.IFaces.utils import id_sort_key


# Последовательный планировщик с учетом зависимостей, но выполняться может только 1 задача,
# с ready-готовых выбираем по ID. планировщик только связывает задачу и раннер, фактическое выполнение лежит внутри runner.exec(task).
class SequentialDepsScheduler(BaseScheduler):
    def __init__(self, *args, runners: Optional[List] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._runners = list(runners) if runners is not None else []

    def get_name(self) -> str:
        return "sequential"

    def get_runners(self) -> List:
        return list(self._runners)

    def set_runners(self, runners: List) -> None:
        self._runners = list(runners)

    def _workers_limit(self) -> int:
        return 1

    # выбор задачи по id из готовых
    def _ready_tuple(self, tid: str, tasks: Dict[str, TaskSpec]) -> Tuple:
        return (id_sort_key(tid),)

    # получаем id задачи
    def _task_id(self, task) -> str:
        if hasattr(task, "get_id"):
            return str(task.get_id())
        if hasattr(task, "task_id"):
            return str(task.task_id)
        if hasattr(task, "id"):
            return str(task.id)
        raise AttributeError("Task must have get_id(), task_id or id")

    # получаем описание задачи
    def _task_spec(self, task) -> TaskSpec:
        if hasattr(task, "get_spec"):
            return task.get_spec()
        if hasattr(task, "spec"):
            return task.spec
        if isinstance(task, TaskSpec):
            return task
        raise AttributeError("Task must have get_spec(), spec or be TaskSpec")

    # сбор данных для запуска: сами объекты задач, которые будут переданы в runner.exec(task) + TaskSpec для анализа зависимостей
    def _collect_tasks(self, tasks: Iterable) -> Tuple[Dict[str, object], Dict[str, TaskSpec]]:
        task_objects: Dict[str, object] = {}
        task_specs: Dict[str, TaskSpec] = {}

        for task in tasks:
            task_id = self._task_id(task)

            if task_id in task_objects:
                raise ValueError(f"Duplicate task id: {task_id}")

            task_objects[task_id] = task
            task_specs[task_id] = self._task_spec(task)

        return task_objects, task_specs

    # смотрим на зависимости (какие еще не закрыты + связи)
    def _build_graph(self, tasks: Dict[str, TaskSpec]) -> Tuple[Dict[str, Set[str]], Dict[str, List[str]]]:
        deps_left: Dict[str, Set[str]] = {}
        children: Dict[str, List[str]] = {tid: [] for tid in tasks}

        for tid, spec in tasks.items():
            deps_left[tid] = set(spec.deps)

        for tid, spec in tasks.items():
            for dep in spec.deps:
                if dep not in tasks:
                    raise ValueError(f"Task {tid} depends on unknown task {dep}")
                children[dep].append(tid)

        return deps_left, children

    # приводим ответ runner.exec(task) к TaskResult
    def _normalize_result(self, task_id: str, started_at: float, raw_result) -> TaskResult:
        if isinstance(raw_result, TaskResult):
            return raw_result

        ended_at = time.time()
        return_code = 0 if raw_result is None else int(raw_result)

        return TaskResult(
            task_id=task_id,
            start_ts=started_at,
            end_ts=ended_at,
            return_code=return_code,
            status="ok" if return_code == 0 else "failed",
        )

    # последовательный цикл. собираем входной набор задач, строим граф, смотрим на очередь готовых задач, запускаем по 1 задаче, потом обновляем граф и меняем статусы потомков. если упала задача -- ее потомки skipped
    #scheduler выбирает задачу; runner выполняет задачу через runner.exec(task)
    def run(self, runners: List, tasks: Iterable) -> Dict[str, TaskResult]:
        if not runners:
            raise ValueError("Sequential scheduler requires at least one runner")

        task_objects, task_specs = self._collect_tasks(tasks)
        if not task_objects:
            return {}

        #последовательный планировщик использует только первый раннер
        runner = runners[0]

        self._set_running()
        try:
            deps_left, children = self._build_graph(task_specs)

            ready: List[Tuple[Tuple, int, str]] = []
            seq = 0

            def push_ready(tid: str) -> None:
                nonlocal seq
                heapq.heappush(ready, (self._ready_tuple(tid, task_specs), seq, tid))
                seq += 1

            # сюда ready изначально попадают только задачи без зависимостей
            for tid, deps in deps_left.items():
                if not deps:
                    push_ready(tid)

            results: Dict[str, TaskResult] = {}
            done_ok: Set[str] = set()
            failed: Set[str] = set()
            skipped: Set[str] = set()

            def skip_descendants(root_tid: str, *, reason: str) -> None:
                stack = list(children[root_tid])
                while stack:
                    tid = stack.pop()
                    if tid in done_ok or tid in failed or tid in skipped:
                        continue

                    skipped.add(tid)
                    now = time.time()
                    results[tid] = TaskResult(
                        task_id=tid,
                        start_ts=now,
                        end_ts=now,
                        return_code=111,
                        status=f"skipped({reason})",
                    )
                    stack.extend(children[tid])

            while ready:
                # одновременно допускается только один процесс/одна задача
                _, _, tid = heapq.heappop(ready)

                if tid in skipped:
                    continue

                started_at = time.time()
                raw_result = runner.exec(task_objects[tid])
                result = self._normalize_result(tid, started_at, raw_result)
                results[tid] = result

                if result.return_code == 0:
                    result.status = "ok"
                    done_ok.add(tid)

                    # успешная задача закрывает одну зависимость у своих потомков
                    for child in children[tid]:
                        if child in skipped:
                            continue
                        deps_left[child].discard(tid)
                        if not deps_left[child]:
                            push_ready(child)
                else:
                    result.status = "failed"
                    failed.add(tid)
                    skip_descendants(tid, reason=f"dep_failed:{tid}")

            missing = set(task_objects.keys()) - set(results.keys())
            if missing:
                raise RuntimeError(
                    "Deadlock: some tasks were never scheduled/finished. "
                    f"Missing={sorted(missing, key=id_sort_key)}"
                )

            self._results = results
            return dict(self._results)
        finally:
            self._set_finished()

    def run_all(self) -> Dict[str, TaskResult]:
        if not self._runners:
            raise RuntimeError(
                "SequentialDepsScheduler.run_all() requires runners. "
                "Use set_runners(runners) before run_all() or call run(runners, tasks)."
            )

        tasks = list(getattr(self, "_tasks", {}).values())
        return self.run(self._runners, tasks)
