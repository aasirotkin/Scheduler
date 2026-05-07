from __future__ import annotations

import inspect
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from Include.Scheduler.IFaces.i_runner import IRunner, RunnerResources
from Include.Scheduler.IFaces.i_task import ITask
from Include.Scheduler.IFaces.task_result import TaskResult

#исполнитель для нескольких задач сразу, хранит аткивные задачи и оценивает по ресурсам запуск следущей
class LocalRunner(IRunner):

    def __init__(
        self,
        runner_id: str,
        *,
        cpu_percent: float,
        mem_mb: float,
        net_mbps: float,
        max_parallel_tasks: Optional[int] = None,
        jitter_pct: float = 0.0,
        seed: Optional[int] = None,
        cpu_weight: float = 1.0,
        mem_weight: float = 1.0,
        net_weight: float = 1.0,
    ):
        self._runner_id = runner_id

        # Полная ёмкость исполнителя
        self._capacity = RunnerResources(
            cpu_percent=cpu_percent,
            mem_mb=mem_mb,
            net_mbps=net_mbps,
        )

        # Дополнительный лимит на количество одновременно активных задач.
        # Если None, ограничение только по ресурсам.
        self._max_parallel_tasks = max_parallel_tasks

        self._jitter_pct = jitter_pct
        self._seed = seed

        #нужно свести к одно чиселке с помощью коэффициентов,свёртка ресурсов в одно число для power()/power_left()
        self._cpu_weight = cpu_weight
        self._mem_weight = mem_weight
        self._net_weight = net_weight

        self._lock = threading.Lock()

        # активные задачи и связанные
        self._active_tasks: Dict[str, ITask] = {}
        self._active_processes: Dict[str, subprocess.Popen] = {}
        self._start_times: Dict[str, float] = {}

        # очередь собранных результатов, которые ещё не забрал scheduler
        self._completed_results: List[TaskResult] = []

#описание раннера
    def get_id(self) -> str:
        return self._runner_id

    def total_resources(self) -> RunnerResources:
        return self._capacity

    def used_resources(self) -> RunnerResources:
        with self._lock:
            return self._used_resources_unlocked()

    def free_resources(self) -> RunnerResources:
        with self._lock:
            return self._free_resources_unlocked()

#числовая мощность
    def power(self) -> float:
        return self._resource_power(self._capacity)

    def power_left(self) -> float:
        with self._lock:
            return self._resource_power(self._free_resources_unlocked())

#состояние раннера
    def active_task_ids(self) -> List[str]:
        with self._lock:
            return list(self._active_tasks.keys())

    def active_task_count(self) -> int:
        with self._lock:
            return len(self._active_tasks)

    def has_running_tasks(self) -> bool:
        with self._lock:
            return len(self._active_tasks) > 0

    def can_run(self, task: ITask) -> bool:
        with self._lock:
            # Если задан лимит по количеству параллельных задач,
            if self._max_parallel_tasks is not None:
                if len(self._active_tasks) >= self._max_parallel_tasks:
                    return False

            #нельзя запускать вторую задачу с тем же id
            task_id = task.get_id()
            if task_id in self._active_tasks:
                return False

            need = self._task_resources(task)
            free = self._free_resources_unlocked()
            return free.fits(need)

    def exec(self, task):
        """
        Выполняет одну задачу локально.

        Планировщик вызывает именно этот метод:
            runner.exec(task)

        Задача должна уметь отдавать путь к скрипту через get_script()
        или иметь поле script.
        """
        if not hasattr(self, "_results"):
            self._results = {}

        task_id = self._get_task_id(task)
        script_path = self._get_task_script(task)

        start_ts = time.time()
        return_code = -1
        output = ""

        try:
            if script_path is None:
                raise ValueError(f"Task {task_id} has no script path")

            script_path = str(script_path)

            if script_path.endswith(".py"):
                command = [sys.executable, script_path]
            elif script_path.endswith(".sh"):
                command = ["/bin/bash", script_path]
            else:
                command = [script_path]

            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
            )

            return_code = process.returncode
            output = (process.stdout or "") + (process.stderr or "")

        except Exception as exc:
            output = str(exc)
            return_code = -1

        end_ts = time.time()

        status = "DONE" if return_code == 0 else "FAILED"

        result = self._make_task_result(
            task_id=task_id,
            start_ts=start_ts,
            end_ts=end_ts,
            return_code=return_code,
            status=status,
            output=output,
        )

        self._results[task_id] = result

        return result

    def _get_task_id(self, task):
        if hasattr(task, "get_id"):
            return str(task.get_id())

        if hasattr(task, "id"):
            return str(task.id)

        if hasattr(task, "task_id"):
            return str(task.task_id)

        raise ValueError("Task has no id")

    def _get_task_script(self, task):
        if hasattr(task, "get_script"):
            return task.get_script()

        if hasattr(task, "script"):
            return task.script

        return None

    def _make_task_result(
        self,
        task_id,
        start_ts,
        end_ts,
        return_code,
        status,
        output,
    ):
        """
        Создаёт TaskResult с учётом того, какие поля реально есть
        в твоём task_result.py.
        """
        signature = inspect.signature(TaskResult)
        kwargs = {}

        values = {
            "task_id": task_id,
            "id": task_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "return_code": return_code,
            "status": status,
            "output": output,
            "message": output,
            "stdout": output,
        }

        for field_name in signature.parameters:
            if field_name in values:
                kwargs[field_name] = values[field_name]

        return TaskResult(**kwargs)

    def get_result(self, task_id):
        if not hasattr(self, "_results"):
            self._results = {}

        return self._results.get(str(task_id))

    def get_results(self):
        if not hasattr(self, "_results"):
            self._results = {}

        return list(self._results.values())

#Запуск новой задачи
    def submit(self, task: ITask) -> None:
        with self._lock:
            task_id = task.get_id()

            if self._max_parallel_tasks is not None:
                if len(self._active_tasks) >= self._max_parallel_tasks:
                    raise RuntimeError(
                        f"Runner {self._runner_id} reached max_parallel_tasks="
                        f"{self._max_parallel_tasks}"
                    )

            if task_id in self._active_tasks:
                raise ValueError(
                    f"Task {task_id} is already running on runner {self._runner_id}"
                )

            need = self._task_resources(task)
            free = self._free_resources_unlocked()
            if not free.fits(need):
                raise ValueError(
                    f"Runner {self._runner_id} cannot run task {task_id}: "
                    f"not enough free resources"
                )

            # Путь до python-скрипта, который реально исполняет задачу
            entrypoint = Path(task.get_entrypoint()).expanduser().resolve()
            if not entrypoint.exists():
                raise FileNotFoundError(f"Task entrypoint not found: {entrypoint}")

            spec = task.get_spec()

            # Формируем команду запуска
            cmd = [
                sys.executable,
                str(entrypoint),
                "--task-id",
                task_id,
                "--mem-mb",
                str(spec.mem_mb),
                "--net-mbps",
                str(spec.net_mbps),
                "--cpu-percent",
                str(spec.cpu_percent),
                "--duration",
                str(spec.duration),
            ]

            if self._jitter_pct > 0:
                cmd += ["--jitter-pct", str(self._jitter_pct)]

            if self._seed is not None:
                cmd += ["--seed", str(self._seed)]

            process = subprocess.Popen(cmd)

            # Сохраняем задачу в набор активных
            self._active_tasks[task_id] = task
            self._active_processes[task_id] = process
            self._start_times[task_id] = time.time()

# cбор завершившихся задач

    def poll_results(self) -> List[TaskResult]:
        with self._lock:
            # проверяем уже накопленные результаты
            ready_results = list(self._completed_results)
            self._completed_results.clear()

            # Копия ключей нужна, потому что в процессе цикла мы будем удалять завершившиеся задачи из словарей
            for task_id in list(self._active_processes.keys()):
                process = self._active_processes[task_id]
                return_code = process.poll()

                # Процесс ещё работает
                if return_code is None:
                    continue

                task = self._active_tasks[task_id]
                start_ts = self._start_times[task_id]
                end_ts = time.time()

                status = "success" if return_code == 0 else "failed"

                ready_results.append(
                    TaskResult(
                        task_id=task_id,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        return_code=return_code,
                        status=status,
                    )
                )

                # удаляем завершившуюся задачу из активных
                del self._active_tasks[task_id]
                del self._active_processes[task_id]
                del self._start_times[task_id]

            return ready_results

    def poll_result(self) -> Optional[TaskResult]:
        results = self.poll_results()
        if not results:
            return None

        # Если пришло несколько результатов сразу,один возвращаем, остальные сохраняем в буфер.
        first = results[0]
        if len(results) > 1:
            with self._lock:
                self._completed_results.extend(results[1:])
        return first

# чп остановка всех активных задач

    def shutdown(self) -> None:
        with self._lock:
            for task_id, process in list(self._active_processes.items()):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2.0)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()

            self._active_tasks.clear()
            self._active_processes.clear()
            self._start_times.clear()
            self._completed_results.clear()

#вспомогательные методы

    def _task_resources(self, task: ITask) -> RunnerResources:
        spec = task.get_spec()
        return RunnerResources(
            cpu_percent=spec.cpu_percent,
            mem_mb=spec.mem_mb,
            net_mbps=spec.net_mbps,
        )

    def _used_resources_unlocked(self) -> RunnerResources:
        used = RunnerResources(cpu_percent=0.0, mem_mb=0.0, net_mbps=0.0)

        for task in self._active_tasks.values():
            used = used.add(self._task_resources(task))

        return used

    def _free_resources_unlocked(self) -> RunnerResources:
        used = self._used_resources_unlocked()
        return self._capacity.sub(used)

    def _resource_power(self, resources: RunnerResources) -> float:
        return (
            self._cpu_weight * resources.cpu_percent
            + self._mem_weight * resources.mem_mb
            + self._net_weight * resources.net_mbps
        )
