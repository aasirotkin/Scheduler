import heapq
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Dict, Iterable, List, Optional, Set, Tuple

from Src.base_scheduler import BaseScheduler
from Include.Scheduler.IFaces.task_result import TaskResult
from Include.Scheduler.IFaces.task_spec import TaskSpec
from Include.Scheduler.IFaces.utils import id_sort_key


# параллельный FIFO-планировщик с учётом зависимостей,может одновременно запускать несколько задач по порядку FIFO/ID
class ParallelFIFODepScheduler(BaseScheduler):

    def __init__(self, *args, runners: Optional[List] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._runners = list(runners) if runners is not None else []

    # получить список раннеров, которые были переданы в __init__ или положены снаружи
    def get_runners(self) -> List:
        return list(self._runners)

    # задать список раннеров после создания планировщика
    def set_runners(self, runners: List) -> None:
        self._runners = list(runners)

    # достаем id задачи из ITask
    def _task_id(self, task) -> str:
        if hasattr(task, "get_id"):
            return str(task.get_id())
        if hasattr(task, "task_id"):
            return str(task.task_id)
        if hasattr(task, "id"):
            return str(task.id)
        raise AttributeError("Task must have get_id(), task_id or id")

    # достаем описание задачи из ITask
    def _task_spec(self, task) -> TaskSpec:
        if hasattr(task, "get_spec"):
            return task.get_spec()
        if hasattr(task, "spec"):
            return task.spec
        if isinstance(task, TaskSpec):
            return task
        raise AttributeError("Task must have get_spec(), spec or be TaskSpec")

    # сбор данных для запуска, раскладываем ITask и TaskSpec по id
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

    # запуск 1 задачи, сам запуск находится  в runner.exec(task)
    def _exec_one(self, runner, task):
        return runner.exec(task)

    def _normalize_result(self, task_id: str, started_at: float, raw_result) -> TaskResult:
        if isinstance(raw_result, TaskResult):
            return raw_result

        now = time.time()
        return_code = 0 if raw_result is None else int(raw_result)

        return TaskResult(
            task_id=task_id,
            start_ts=started_at,
            end_ts=now,
            return_code=return_code,
            status="ok" if return_code == 0 else "failed",
        )

    def get_name(self) -> str:
        return "parallel"

    # количество одновременно решаемых задач определяем числом исполнителей
    def _workers_limit(self) -> int:
        return len(self._runners) if self._runners else self._workers

    # первой запускается задача по порядку ID
    def _ready_tuple(self, tid: str, tasks: Dict[str, TaskSpec]) -> Tuple:
        return (id_sort_key(tid),)

    def _run_with_ready_queue(self, runners: List, task_objects: Dict[str, object], tasks: Dict[str, TaskSpec], ready_key_builder) -> Dict[str, TaskResult]:
        deps_left, children = self._build_graph(tasks)

        ready: List[Tuple[Tuple, int, str]] = []
        seq = 0

        def push_ready(tid: str) -> None:
            nonlocal seq
            heapq.heappush(ready, (ready_key_builder(tid), seq, tid))
            seq += 1

        for tid, deps in deps_left.items():
            if not deps:
                push_ready(tid)

        results: Dict[str, TaskResult] = {}
        done_ok: Set[str] = set()
        failed: Set[str] = set()
        skipped: Set[str] = set()

        free_runners: List = list(runners)
        running: Dict[Future, Tuple[str, object, float]] = {}

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

        def submit_task(executor: ThreadPoolExecutor, runner, tid: str) -> None:
            task = task_objects[tid]
            started_at = time.time()
            future = executor.submit(self._exec_one, runner, task)
            running[future] = (tid, runner, started_at)

            results[tid] = TaskResult(
                task_id=tid,
                start_ts=started_at,
                end_ts=-1.0,
                return_code=-999,
                status="running",
            )

        with ThreadPoolExecutor(max_workers=len(runners)) as executor:
            while ready or running:
                while ready and free_runners:
                    _, _, tid = heapq.heappop(ready)

                    if tid in skipped:
                        continue

                    runner = free_runners.pop(0)
                    submit_task(executor, runner, tid)

                if not running:
                    break

                finished, _ = wait(running.keys(), return_when=FIRST_COMPLETED)

                for future in finished:
                    tid, runner, started_at = running.pop(future)
                    free_runners.append(runner)

                    try:
                        raw_result = future.result()
                        res = self._normalize_result(tid, started_at, raw_result)
                    except Exception:
                        now = time.time()
                        res = TaskResult(
                            task_id=tid,
                            start_ts=started_at,
                            end_ts=now,
                            return_code=1,
                            status="failed",
                        )

                    results[tid] = res

                    if res.return_code == 0:
                        res.status = "ok"
                        done_ok.add(tid)

                        for child in children[tid]:
                            if child in skipped:
                                continue
                            deps_left[child].discard(tid)
                            if not deps_left[child]:
                                push_ready(child)
                    else:
                        res.status = "failed"
                        failed.add(tid)
                        skip_descendants(tid, reason=f"dep_failed:{tid}")

        missing = set(tasks.keys()) - set(results.keys())
        if missing:
            raise RuntimeError(
                "Deadlock: some tasks were never scheduled/finished. "
                f"Missing={sorted(missing, key=id_sort_key)}"
            )

        self._results = results
        return dict(self._results)

    # параллельный цикл. заполняем все свободные воркеры, после завершения задачи закрываем зависимости у потомков и двигаем их в ready
    def run(self, runners: List, tasks: Iterable) -> Dict[str, TaskResult]:
        if not runners:
            raise ValueError("Parallel scheduler requires at least one runner")

        task_objects, task_specs = self._collect_tasks(tasks)
        if not task_objects:
            return {}

        self._set_running()
        try:
            return self._run_with_ready_queue(
                runners=runners,
                task_objects=task_objects,
                tasks=task_specs,
                ready_key_builder=lambda tid: self._ready_tuple(tid, task_specs),
            )
        finally:
            self._set_finished()

    def run_all(self) -> Dict[str, TaskResult]:
        if not self._tasks:
            return {}
        if not self._runners:
            raise RuntimeError("No runners were passed. Use run(runners, tasks) or set_runners(runners).")

        return self.run(self._runners, list(self._tasks.values()))
