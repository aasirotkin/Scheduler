import heapq
import subprocess
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

from Src.base_scheduler import BaseScheduler
from Include.Scheduler.IFaces.task_result import TaskResult
from Include.Scheduler.IFaces.task_spec import TaskSpec
from Include.Scheduler.IFaces.utils import id_sort_key


# параллельный FIFO-планировщик с учётом зависимостей,
# может одновременно запускать несколько задач по порядку FIFO/ID
class ParallelFIFODepScheduler(BaseScheduler):
    def get_name(self) -> str:
        return "parallel"

    # количество одновременно решаемых задач определяем числом исполнителей
    def _workers_limit(self) -> int:
        return self._workers

    # первой запускается задача по порядку ID
    def _ready_tuple(self, tid: str, tasks: Dict[str, TaskSpec]) -> Tuple:
        return (id_sort_key(tid),)

    # сбор данных для запуска
    def _collect_payload(self) -> Tuple[str, Dict[str, TaskSpec]]:
        tasks: Dict[str, TaskSpec] = {}
        task_py: Optional[str] = None

        for task in self._tasks.values():
            task_id = task.get_id()
            tasks[task_id] = task.get_spec()

            entrypoint = task.get_entrypoint()
            if task_py is None:
                task_py = entrypoint
            elif task_py != entrypoint:
                raise ValueError("All queued tasks must use the same task entrypoint")

        if task_py is None:
            raise RuntimeError("Task entrypoint is not defined")

        return task_py, tasks

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

    # запуск 1 задачи
    def _popen_one(self, task_py: str, task_id: str, spec: TaskSpec) -> subprocess.Popen:
        cmd = [
            sys.executable,
            task_py,
            "--task-id", str(task_id),
            "--mem-mb", str(spec.mem_mb),
            "--net-mbps", str(spec.net_mbps),
            "--cpu-percent", str(spec.cpu_percent),
            "--duration", str(spec.duration),
        ]

        if self._jitter_pct > 0:
            cmd += ["--jitter-pct", str(self._jitter_pct)]

        if self._seed is not None:
            cmd += ["--seed", str(self._seed)]

        return subprocess.Popen(cmd)

    # параллельный цикл. заполняем все свободные worker-slot,
    # после завершения задачи закрываем зависимости у потомков и двигаем их в ready
    def run_all(self) -> Dict[str, TaskResult]:
        if not self._tasks:
            return {}

        self._set_running()
        try:
            task_py, tasks = self._collect_payload()
            deps_left, children = self._build_graph(tasks)

            ready: List[Tuple[Tuple, int, str]] = []
            seq = 0

            def push_ready(tid: str) -> None:
                nonlocal seq
                heapq.heappush(ready, (self._ready_tuple(tid, tasks), seq, tid))
                seq += 1

            for tid, deps in deps_left.items():
                if not deps:
                    push_ready(tid)

            running: Dict[subprocess.Popen, str] = {}
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

            while ready or running:
                while len(running) < self._workers_limit() and ready:
                    _, _, tid = heapq.heappop(ready)

                    if tid in skipped:
                        continue

                    process = self._popen_one(task_py, tid, tasks[tid])
                    running[process] = tid

                    now = time.time()
                    results[tid] = TaskResult(
                        task_id=tid,
                        start_ts=now,
                        end_ts=-1.0,
                        return_code=-999,
                        status="running",
                    )

                time.sleep(0.05)

                finished: List[Tuple[subprocess.Popen, str, int]] = []
                for process, tid in list(running.items()):
                    rc = process.poll()
                    if rc is not None:
                        finished.append((process, tid, int(rc)))

                for process, tid, rc in finished:
                    del running[process]

                    now = time.time()
                    res = results[tid]
                    res.end_ts = now
                    res.return_code = rc

                    if rc == 0:
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
        finally:
            self._set_finished()
