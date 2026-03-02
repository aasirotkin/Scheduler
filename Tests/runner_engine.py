#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import time
import heapq
import subprocess
from typing import Callable, Dict, List, Optional, Set, Tuple, Any

from task_models import TaskResult, TaskSpec, _id_sort_key

#открываем tasks.json и возвращаем словарь id -> TaskSpec


def load_tasks(path: str) -> Dict[str, TaskSpec]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks: Dict[str, TaskSpec] = {}
    for t in data["tasks"]:
        spec = TaskSpec(
            id=str(t["id"]),
            mem_mb=float(t["mem_mb"]),
            net_mbps=float(t["net_mbps"]),
            cpu_percent=float(t["cpu_percent"]),
            duration=float(t["duration"]),
            priority=int(t.get("priority", 0)),
            deps=tuple(str(x) for x in t.get("deps", [])),
        )
        if spec.id in tasks:
            raise ValueError(f"Duplicate task id: {spec.id}")
        tasks[spec.id] = spec

    #еррор при несушествовании зависимости, каждая зависимость должна существовать
    for tid, spec in tasks.items():
        for d in spec.deps:
            if d not in tasks:
                raise ValueError(f"Task {tid} depends on unknown task {d}")

    return tasks


#запуск одной задачи
def _popen_one(task_py: str, spec: TaskSpec, *, jitter_pct: float = 0.0, seed: Optional[int] = None) -> subprocess.Popen:
    cmd = [
        sys.executable, task_py,
        "--task-id", str(spec.id),
        "--mem-mb", str(spec.mem_mb),
        "--net-mbps", str(spec.net_mbps),
        "--cpu-percent", str(spec.cpu_percent),
        "--duration", str(spec.duration),
    ]
    if jitter_pct and jitter_pct > 0:
        cmd += ["--jitter-pct", str(jitter_pct)]
    if seed is not None:
        cmd += ["--seed", str(seed)]

    return subprocess.Popen(cmd)


# граф зависимостей, топологическая сортировка

KeyFn = Callable[[str], Tuple]  # тип функции ключа для heap. heap key (min-heap)


# создаем 2 структуры --незакрытые зависимости и список тех, кто зависиит от этой задачи
def _build_graph(tasks: Dict[str, TaskSpec]) -> Tuple[Dict[str, Set[str]], Dict[str, List[str]]]:
    deps_left: Dict[str, Set[str]] = {tid: set(spec.deps) for tid, spec in tasks.items()}
    children: Dict[str, List[str]] = {tid: [] for tid in tasks}
    for tid, spec in tasks.items():
        for d in spec.deps:
            children[d].append(tid)
    return deps_left, children


#топологическая сортировка
def _compute_topological_order(tasks: Dict[str, TaskSpec]) -> List[str]:
    """Kahn topological sort. Бросает ValueError при цикле."""
    indeg: Dict[str, int] = {tid: 0 for tid in tasks}
    children: Dict[str, List[str]] = {tid: [] for tid in tasks}
    for tid, spec in tasks.items():
        for d in spec.deps:
            indeg[tid] += 1
            children[d].append(tid)

    q: List[str] = [tid for tid, deg in indeg.items() if deg == 0]
    q.sort(key=_id_sort_key)

    topo: List[str] = []
    while q:
        tid = q.pop(0)
        topo.append(tid)
        for ch in children[tid]:
            indeg[ch] -= 1
            if indeg[ch] == 0:
                q.append(ch)
                q.sort(key=_id_sort_key)

    if len(topo) != len(tasks):
        raise ValueError("Cycle detected in task dependencies (topological sort failed).")
    return topo


#критический путь
def compute_blevel(tasks: Dict[str, TaskSpec]) -> Dict[str, float]:
    #b-level (bottom level) = duration(node) + max_{child}(b-level(child)).
    topo = _compute_topological_order(tasks)

    # children
    children: Dict[str, List[str]] = {tid: [] for tid in tasks}
    for tid, spec in tasks.items():
        for d in spec.deps:
            children[d].append(tid)

    b: Dict[str, float] = {tid: float(tasks[tid].duration) for tid in tasks}
    for tid in reversed(topo):
        if children[tid]:
            b[tid] = float(tasks[tid].duration) + max(b[ch] for ch in children[tid])
        else:
            b[tid] = float(tasks[tid].duration)
    return b


#ГЛАВНЫЙ "ДВИЖОК", отличается только политикой выбора следущей готовой задачи (ready_key)
# workers - сколько параллельный процессов
# ready_key -- как выбираем след задачу
# poll_s -- частота опроса процессоров
def run_with_deps(
    task_py: str,
    tasks: Dict[str, TaskSpec],
    *,
    workers: int,
    ready_key: KeyFn,
    jitter_pct: float = 0.0,
    seed: Optional[int] = None,
    poll_s: float = 0.05,
) -> Dict[str, TaskResult]:

    if workers < 1: # ну а как без воркеров работать аххха
        raise ValueError("workers must be >= 1")
    #строим граф
    deps_left, children = _build_graph(tasks)

    ready: List[Tuple[Tuple, int, str]] = []  # heap из набора (key, seq, tid)
    seq = 0 #чтобы при равных ключах сохранять порядок 

    def push_ready(tid: str):
        nonlocal seq
        heapq.heappush(ready, (ready_key(tid), seq, tid))
        seq += 1

    # стартовые ready узлы
    for tid, deps in deps_left.items():
        if not deps:
            push_ready(tid)

    #состояние рантайма
    running: Dict[subprocess.Popen, str] = {} # какие процессы сейчас исполняются
    results: Dict[str, TaskResult] = {} # все резы
    done_ok: Set[str] = set()
    failed: Set[str] = set()
    skipped: Set[str] = set()

    start_all = time.time()

    #если упала задача, потомки не могут выполнить зависимости. тогда обходим всех потомков и добавляем фиктивный результат
    def skip_descendants(root_tid: str, *, reason: str):
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
                return_code=111,  # условный код "skipped"
                status=f"skipped({reason})",
            )
            stack.extend(children[tid])

    # Основной цикл
    while ready or running:
        # заполняем свободных воркеров. берем задачу reday из heap, запускаем и пишем "running"
        while len(running) < workers and ready:
            _, _, tid = heapq.heappop(ready)

            # Если задачу уже пометили skipped— не запускаем.
            if tid in skipped:
                continue

            spec = tasks[tid]
            p = _popen_one(task_py, spec, jitter_pct=jitter_pct, seed=seed)
            running[p] = tid
            now = time.time()
            results[tid] = TaskResult(task_id=tid, start_ts=now, end_ts=-1.0, return_code=-999, status="running")

        #небольшой слип
        time.sleep(poll_s)

        #собираем завершившиеся процессы через poll
        finished: List[Tuple[subprocess.Popen, str, int]] = []
        for p, tid in list(running.items()):
            rc = p.poll()
            if rc is not None:
                finished.append((p, tid, int(rc)))

        #обработка завершившихся. берем из runnung заполняем время конца и результата, закрываем зависимости и переводим в ready
        for p, tid, rc in finished:
            del running[p]
            now = time.time()
            res = results[tid]
            res.end_ts = now
            res.return_code = rc
            if rc == 0:
                res.status = "ok"
                done_ok.add(tid)

                #успешное завершение "закрывает" зависимости потомков
                for ch in children[tid]:
                    if ch in skipped:
                        continue
                    deps_left[ch].discard(tid)
                    if not deps_left[ch]:
                        push_ready(ch)
            else:
                res.status = "failed"
                failed.add(tid)
                # Потомков помечаем как skipped
                skip_descendants(tid, reason=f"dep_failed:{tid}")

    end_all = time.time()
    makespan = end_all - start_all
    print(f"\n[SUMMARY] makespan={makespan:.3f}s workers={workers}")

    #поиск пропущенных.  Если остались задачи, которых нет в results — это означает цикл или невыполнимые deps
    missing = set(tasks.keys()) - set(results.keys())
    if missing:
        # Если у нас были failures, часть задач могла оказаться не помеченной skipped 
        raise RuntimeError(
            "Deadlock: some tasks were never scheduled/finished. "
            f"Missing={sorted(missing, key=_id_sort_key)}. "
            "Проверьте циклы в deps или логику зависимостей."
        )

    return results


# CSV


def save_csv(path: str, results: Dict[str, TaskResult]):
    with open(path, "w", encoding="utf-8") as f:
        f.write("task_id,start_ts,end_ts,duration_s,return_code,status\n")
        for tid in sorted(results.keys(), key=_id_sort_key):
            r = results[tid]
            dur = r.duration_s if r.end_ts >= r.start_ts else ""
            f.write(f"{tid},{r.start_ts:.6f},{r.end_ts:.6f},{dur},{r.return_code},{r.status}\n")
    print(f"[OUT] CSV saved: {path}")
