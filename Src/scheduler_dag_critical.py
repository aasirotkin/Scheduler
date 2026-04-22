import heapq
import subprocess
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

from Src.base_scheduler import BaseScheduler
from Include.Scheduler.IFaces.task_result import TaskResult
from Include.Scheduler.IFaces.task_spec import TaskSpec
from Include.Scheduler.IFaces.utils import id_sort_key


# планировшик по критичекому пути, раньше запускаем те задачи, которые сильнее всего влия/т на обшее время выполнения.  из готовых задач ьерем по коэффициенты blevel + сортировка по приоритету
class DagCriticalPathScheduler(BaseScheduler):
    def get_name(self) -> str:
        return "dag_critical"

    def _workers_limit(self) -> int:
        return self._workers

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

# строим топологический граф, считаем кожффициенты blevel потомков раньше родителей 
    def _compute_topological_order(self, tasks: Dict[str, TaskSpec]) -> List[str]:
        indeg: Dict[str, int] = {tid: 0 for tid in tasks}
        children: Dict[str, List[str]] = {tid: [] for tid in tasks}

        for tid, spec in tasks.items():
            for dep in spec.deps:
                indeg[tid] += 1
                children[dep].append(tid)

        queue = [tid for tid, deg in indeg.items() if deg == 0]
        queue.sort(key=id_sort_key)

        topo: List[str] = []
        while queue:
            tid = queue.pop(0)
            topo.append(tid)

            for child in children[tid]:
                indeg[child] -= 1
                if indeg[child] == 0:
                    queue.append(child)
                    queue.sort(key=id_sort_key)

        if len(topo) != len(tasks):
            raise ValueError("Cycle detected in task dependencies.")

        return topo

# считаем blevel, чем больше коэффициент, тем длиннее хвостик задачи и тем ближе к критическому пути
    def _compute_blevel(self, tasks: Dict[str, TaskSpec]) -> Dict[str, float]:
        topo = self._compute_topological_order(tasks)

        children: Dict[str, List[str]] = {tid: [] for tid in tasks}
        for tid, spec in tasks.items():
            for dep in spec.deps:
                children[dep].append(tid)

        blevel: Dict[str, float] = {tid: float(tasks[tid].duration) for tid in tasks}

        for tid in reversed(topo):
            if children[tid]:
                blevel[tid] = float(tasks[tid].duration) + max(
                    blevel[child] for child in children[tid]
                )
            else:
                blevel[tid] = float(tasks[tid].duration)

        return blevel

# сравниваем готовые задачи: больше b-level -> раньше запуск + приоритеты, если равны к-ты
    def _ready_tuple(self, tid: str, tasks: Dict[str, TaskSpec], blevel: Dict[str, float]) -> Tuple:
        return (-blevel[tid], -tasks[tid].priority, id_sort_key(tid))

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

    def run_all(self) -> Dict[str, TaskResult]:
        if not self._tasks:
            return {}

        task_py, tasks = self._collect_payload()
        deps_left, children = self._build_graph(tasks)
        blevel = self._compute_blevel(tasks)

        ready: List[Tuple[Tuple, int, str]] = []
        seq = 0

        def push_ready(tid: str) -> None:
            nonlocal seq
            heapq.heappush(ready, (self._ready_tuple(tid, tasks, blevel), seq, tid))
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
