from __future__ import annotations

import subprocess
import sys
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Any, Optional

try:
    import Include.Scheduler.IFaces.task_result as task_result_module
    from Include.Scheduler.IFaces.task_result import TaskResult
except ImportError as exc:
    raise ImportError("Не удалось импортировать TaskResult") from exc


class RunnerFactoryHelper:
    @staticmethod
    def create_process_runner(
        runner_id: str = "local-1",
        cpu_percent: float = 100.0,
        mem_mb: float = 1024.0,
        net_mbps: float = 200.0,
        max_parallel_tasks: int = 1,
        jitter_pct: float = 0.0,
        seed: Optional[int] = None,
        cpu_weight: float = 1.0,
        mem_weight: float = 1.0,
        net_weight: float = 1.0,
    ) -> Any:
        return _ProcessRunner(
            runner_id=runner_id,
            cpu_percent=cpu_percent,
            mem_mb=mem_mb,
            net_mbps=net_mbps,
            max_parallel_tasks=max_parallel_tasks,
            jitter_pct=jitter_pct,
            seed=seed,
            cpu_weight=cpu_weight,
            mem_weight=mem_weight,
            net_weight=net_weight,
        )


class _ProcessRunner:
    def __init__(
        self,
        runner_id: str = "local-1",
        cpu_percent: float = 100.0,
        mem_mb: float = 1024.0,
        net_mbps: float = 200.0,
        max_parallel_tasks: int = 1,
        jitter_pct: float = 0.0,
        seed: Optional[int] = None,
        cpu_weight: float = 1.0,
        mem_weight: float = 1.0,
        net_weight: float = 1.0,
    ) -> None:
        self.runner_id = str(runner_id)
        self.id = self.runner_id
        self.name = self.runner_id

        self.cpu_percent = float(cpu_percent)
        self.mem_mb = float(mem_mb)
        self.net_mbps = float(net_mbps)
        self.max_parallel_tasks = max(1, int(max_parallel_tasks))

        # параметры оставлены, чтобы фабрика могла передавать их без ошибки
        self.jitter_pct = float(jitter_pct)
        self.seed = seed
        self.cpu_weight = float(cpu_weight)
        self.mem_weight = float(mem_weight)
        self.net_weight = float(net_weight)

        self._lock = threading.Lock()
        self._active_tasks = 0
        self._used_cpu_percent = 0.0
        self._used_mem_mb = 0.0
        self._used_net_mbps = 0.0
        self._results: dict[str, TaskResult] = {}

    def get_id(self) -> str:
        return self.runner_id

    def get_name(self) -> str:
        return self.runner_id

    def _task_id(self, task: Any) -> str:
        if hasattr(task, "get_id"):
            return str(task.get_id())

        for attr_name in ("task_id", "id", "name"):
            if hasattr(task, attr_name):
                value = getattr(task, attr_name)

                if value is not None:
                    return str(value)

        return "unknown-task"

    def _task_script(self, task: Any) -> str:
        if hasattr(task, "get_script"):
            return str(task.get_script())

        if hasattr(task, "script"):
            return str(task.script)

        if hasattr(task, "entrypoint"):
            return str(task.entrypoint)

        raise ValueError(f"Task {self._task_id(task)} does not contain script path")

    def _task_spec(self, task: Any) -> Any:
        if hasattr(task, "get_spec"):
            return task.get_spec()

        if hasattr(task, "spec"):
            return task.spec

        return task

    def _required_cpu(self, task: Any) -> float:
        spec = self._task_spec(task)
        return float(getattr(spec, "cpu_percent", getattr(task, "cpu_percent", 0.0)))

    def _required_mem(self, task: Any) -> float:
        spec = self._task_spec(task)
        return float(getattr(spec, "mem_mb", getattr(task, "mem_mb", 0.0)))

    def _required_net(self, task: Any) -> float:
        spec = self._task_spec(task)
        return float(getattr(spec, "net_mbps", getattr(task, "net_mbps", 0.0)))

    # проверяем, хватает ли у раннера ресурсов на задачу
    def can_run(self, task: Any) -> bool:
        required_cpu = self._required_cpu(task)
        required_mem = self._required_mem(task)
        required_net = self._required_net(task)

        with self._lock:
            if self._active_tasks >= self.max_parallel_tasks:
                return False

            return (
                self._used_cpu_percent + required_cpu <= self.cpu_percent
                and self._used_mem_mb + required_mem <= self.mem_mb
                and self._used_net_mbps + required_net <= self.net_mbps
            )

    def fits(self, task: Any) -> bool:
        return self.can_run(task)

    def _reserve(self, task: Any) -> bool:
        required_cpu = self._required_cpu(task)
        required_mem = self._required_mem(task)
        required_net = self._required_net(task)

        with self._lock:
            if self._active_tasks >= self.max_parallel_tasks:
                return False

            if self._used_cpu_percent + required_cpu > self.cpu_percent:
                return False

            if self._used_mem_mb + required_mem > self.mem_mb:
                return False

            if self._used_net_mbps + required_net > self.net_mbps:
                return False

            self._active_tasks += 1
            self._used_cpu_percent += required_cpu
            self._used_mem_mb += required_mem
            self._used_net_mbps += required_net

            return True

    def _release(self, task: Any) -> None:
        with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            self._used_cpu_percent = max(0.0, self._used_cpu_percent - self._required_cpu(task))
            self._used_mem_mb = max(0.0, self._used_mem_mb - self._required_mem(task))
            self._used_net_mbps = max(0.0, self._used_net_mbps - self._required_net(task))

    def power(self) -> int:
        return int(self.cpu_percent * self.cpu_weight + self.mem_mb * self.mem_weight + self.net_mbps * self.net_weight)

    def power_left(self) -> int:
        with self._lock:
            cpu_left = self.cpu_percent - self._used_cpu_percent
            mem_left = self.mem_mb - self._used_mem_mb
            net_left = self.net_mbps - self._used_net_mbps

        return int(cpu_left * self.cpu_weight + mem_left * self.mem_weight + net_left * self.net_weight)

    def powerleft(self) -> int:
        return self.power_left()

    def _command_for_script(self, script: str) -> list[str]:
        script_path = Path(script)
        suffix = script_path.suffix.lower()

        if suffix == ".py":
            return [sys.executable, str(script_path)]

        if suffix == ".sh":
            return ["/bin/sh", str(script_path)]

        return [str(script_path)]

    def _status_value(self, ok: bool) -> Any:
        enum_candidates = [
            getattr(task_result_module, "TaskStatus", None),
            getattr(task_result_module, "ResultStatus", None),
        ]
        names = ("DONE", "SUCCESS", "FINISHED", "OK") if ok else ("FAILED", "ERROR")

        for enum_class in enum_candidates:
            if enum_class is None:
                continue

            try:
                if not issubclass(enum_class, Enum):
                    continue
            except TypeError:
                continue

            for name in names:
                if hasattr(enum_class, name):
                    return getattr(enum_class, name)

        return "done" if ok else "failed"

    def _make_result(
        self,
        task_id: str,
        start_ts: float,
        end_ts: float,
        return_code: int,
        stdout: str = "",
        stderr: str = "",
    ) -> TaskResult:
        ok = return_code == 0
        values: dict[str, Any] = {
            "task_id": task_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "return_code": return_code,
            "status": self._status_value(ok),
            "stdout": stdout,
            "stderr": stderr,
            "message": stderr if stderr else stdout,
            "runner_id": self.runner_id,
            "duration_s": end_ts - start_ts,
        }

        try:
            return TaskResult(
                task_id=task_id,
                start_ts=start_ts,
                end_ts=end_ts,
                return_code=return_code,
                status=values["status"],
                stdout=stdout,
                stderr=stderr,
                message=values["message"],
                runner_id=self.runner_id,
                duration_s=end_ts - start_ts,
            )
        except TypeError:
            return TaskResult(
                task_id=task_id,
                start_ts=start_ts,
                end_ts=end_ts,
                return_code=return_code,
                status=values["status"],
            )

    # запускаем задачу и сохраняем результат выполнения
    def exec(self, task: Any) -> TaskResult:
        task_id = self._task_id(task)
        script = self._task_script(task)

        if not self._reserve(task):
            now = time.time()
            result = self._make_result(
                task_id=task_id,
                start_ts=now,
                end_ts=now,
                return_code=1,
                stderr="Недостаточно ресурсов раннера для запуска задачи",
            )
            self._results[task_id] = result
            return result

        start_ts = time.time()
        return_code = 1
        stdout = ""
        stderr = ""

        try:
            completed = subprocess.run(
                self._command_for_script(script),
                capture_output=True,
                text=True,
                check=False,
            )
            return_code = int(completed.returncode)
            stdout = completed.stdout or ""
            stderr = completed.stderr or ""
        except Exception as exc:
            stderr = str(exc)
            return_code = 1
        finally:
            end_ts = time.time()
            self._release(task)

        result = self._make_result(
            task_id=task_id,
            start_ts=start_ts,
            end_ts=end_ts,
            return_code=return_code,
            stdout=stdout,
            stderr=stderr,
        )
        self._results[task_id] = result
        return result

    def run(self, task: Any) -> TaskResult:
        return self.exec(task)

    def result(self, task_id: str) -> Optional[TaskResult]:
        return self._results.get(str(task_id))

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        return self.result(task_id)

    def get_results(self) -> dict[str, TaskResult]:
        return dict(self._results)
