from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from Include.Scheduler.IFaces.task_result import TaskResult
from Include.Scheduler.IFaces.task_spec import TaskSpec


# импорт фабрики раннера
try:
    from Include.Scheduler.runner_factory import RunnerFactory
except ImportError as exc:
    raise ImportError(
        "Не удалось импортировать RunnerFactory"
    ) from exc


# импорт фабрики планировщика
try:
    from Include.Scheduler.scheduler_factory import SchedulerFactory, SchedulerKind
except ImportError as exc:
    raise ImportError(
        "Не удалось импортировать SchedulerFactory"
    ) from exc


@dataclass(frozen=True)
class MainConfig:
    root_dir: Path
    tasks_file: Path
    results_file: Path
    trace_file: Path
    metrics_file: Path
    runner_count: int
    schedulers_to_test: Sequence[tuple[str, str]]


# объект задачи для запуска
@dataclass
class TaskForRun:
    task_id: str
    script: str
    spec: TaskSpec
    duration: float
    priority: int
    deps: tuple[str, ...]
    cpu_percent: float
    mem_mb: float
    net_mbps: float

    @property
    def id(self) -> str:
        return self.task_id

    def get_id(self) -> str:
        return self.task_id

    def get_script(self) -> str:
        return self.script

    def get_spec(self) -> TaskSpec:
        return self.spec

    def get_duration(self) -> float:
        return self.duration

    def get_priority(self) -> int:
        return self.priority

    def get_dependencies(self) -> tuple[str, ...]:
        return self.deps

    def get_deps(self) -> tuple[str, ...]:
        return self.deps


class TaskFileLoader:
    def __init__(self, config: MainConfig) -> None:
        self._config = config
        self._resolved_tasks_file: Path | None = None

    def get_tasks_file(self) -> Path:
        if self._resolved_tasks_file is not None:
            return self._resolved_tasks_file

        return self._resolve_tasks_file()

    def _resolve_tasks_file(self) -> Path:
        if self._config.tasks_file.exists():
            self._resolved_tasks_file = self._config.tasks_file
            return self._resolved_tasks_file

        tests_dir = self._config.root_dir / "Tests"

        for candidate in sorted(tests_dir.glob("*tasks.json")):
            if candidate.is_file():
                self._resolved_tasks_file = candidate
                return self._resolved_tasks_file

        json_files = ", ".join(path.name for path in sorted(tests_dir.glob("*.json")))

        if json_files:
            raise FileNotFoundError(
                "Не найден файл задач. "
                f"Проверенные json-файлы в Tests: {json_files}"
            )

        raise FileNotFoundError(
            "Не найден файл задач в папке Tests"
        )

    # загрузка задач
    def load_task_rows(self) -> list[dict[str, Any]]:
        tasks_file = self._resolve_tasks_file()

        with tasks_file.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, dict) and "tasks" in data:
            tasks = data["tasks"]

            if not isinstance(tasks, list):
                raise ValueError(
                    "Поле tasks в файле задач должно быть списком."
                )

            return tasks

        if isinstance(data, list):
            return data

        raise ValueError(
            "Неверный формат файла задач. "
            "Ожидается либо список задач, либо объект с полем tasks."
        )

    # определяем путь к скрипту задачи из json или строим его(если нет поля "script")
    def get_script_path(self, row: dict[str, Any]) -> Path:
        if "script" in row and row["script"]:
            return self._config.root_dir / row["script"]

        task_id = str(row["id"])
        return self._config.root_dir / "Tests" / "task_scripts" / f"task_{task_id}.py"

    # генерация скриптов задачи, функция проходит по всем задачас и создает для каждой отдельный скрипт
    # каждая задача получает свой исполняемый файл с нужными ей параметрами памяти, длительности и цпу
    def ensure_task_scripts(self, task_rows: list[dict[str, Any]]) -> None:
        for row in task_rows:
            task_id = str(row["id"])
            script_path = self.get_script_path(row)

            script_path.parent.mkdir(parents=True, exist_ok=True)
            script_text = self._make_script_text(row=row, task_id=task_id)

            # перезаписываем автоматически сгенерированные при запуске скрипты
            script_path.write_text(script_text, encoding="utf-8")
            os.chmod(script_path, 0o755)

    def _make_script_text(self, row: dict[str, Any], task_id: str) -> str:
        return f'''#!/usr/bin/env python3

import time


TASK_ID = "{task_id}"
DURATION = {float(row["duration"])}
CPU_PERCENT = {float(row["cpu_percent"])}
MEM_MB = {float(row["mem_mb"])}


def main():
    # имитация потребления памяти
    # фактическое выделение ограничено, чтобы тест не перегружал мак
    data = bytearray(int(min(MEM_MB, 32)) * 1024 * 1024)

    start = time.perf_counter()
    deadline = start + DURATION

    work_period = 0.05
    busy_part = work_period * max(0.0, min(CPU_PERCENT, 100.0)) / 100.0 # имитация цпу нагрузки через чередлвание сна и вычисоений

    checksum = 0

    while time.perf_counter() < deadline:
        busy_deadline = time.perf_counter() + busy_part

        while time.perf_counter() < busy_deadline:
            checksum = (checksum * 31 + 7) % 1000003

        sleep_time = max(0.0, work_period - busy_part)

        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"task={{TASK_ID}} finished; checksum={{checksum}}; memory={{len(data)}}")


if __name__ == "__main__":
    main()
'''

    # функция делает объект спецификации задачи
    def make_task_spec(self, row: dict[str, Any]) -> TaskSpec:
        deps = tuple(str(dep) for dep in row.get("deps", []))

        #сбор данных
        return TaskSpec(
            cpu_percent=float(row["cpu_percent"]),
            mem_mb=float(row["mem_mb"]),
            net_mbps=float(row["net_mbps"]),
            duration=float(row["duration"]),
            priority=int(row["priority"]),
            deps=deps,
        )

    # сборка задач, готовим объекты для передачи планировщикам
    def build_tasks(self, task_rows: list[dict[str, Any]]) -> list[TaskForRun]:
        tasks: list[TaskForRun] = []

        for row in task_rows:
            task_id = str(row["id"])
            deps = tuple(str(dep) for dep in row.get("deps", []))
            script_path = self.get_script_path(row)
            spec = self.make_task_spec(row)

            tasks.append(
                TaskForRun(
                    task_id=task_id,
                    script=str(script_path),
                    spec=spec,
                    duration=float(row["duration"]),
                    priority=int(row["priority"]),
                    deps=deps,
                    cpu_percent=float(row["cpu_percent"]),
                    mem_mb=float(row["mem_mb"]),
                    net_mbps=float(row["net_mbps"]),
                )
            )

        return tasks


class RunnerBuilder:
    def __init__(self, config: MainConfig) -> None:
        self._config = config

    # создание раннеров через фабрику
    def create_runners(self) -> list[Any]:
        runners = []

        for index in range(self._config.runner_count):
            runner_id = f"local-{index + 1}"

            runner = RunnerFactory.create_local_runner(
                runner_id=runner_id,
                cpu_percent=100,
                mem_mb=1024,
                net_mbps=200,
                max_parallel_tasks=1,
            )

            runners.append(runner)

        return runners


class SchedulerLauncher:
    @staticmethod
    # определяем тип планировщика
    def resolve_scheduler_kind(kind_name: str) -> SchedulerKind:
        return getattr(SchedulerKind, kind_name)

    @staticmethod
    # создание планировщика из фабрики
    def create_scheduler(
        kind: SchedulerKind,
        runners: list[Any],
        tasks: list[TaskForRun],
    ) -> Any:
        return SchedulerFactory.create_scheduler(
            kind=kind,
            tasks=tasks,
            runners=runners,
        )

    @staticmethod
    # запуск планировщика возвращаем время выполнения 
    def run_scheduler(scheduler: Any) -> float:
        start = time.perf_counter()
        scheduler.run_all()
        end = time.perf_counter()

        return end - start

    @staticmethod
    # сборрезультатов у планировщиков/раннеров
    def collect_results(scheduler: Any) -> list[TaskResult]:
        raw_results = scheduler.get_results()

        if raw_results is None:
            return []

        if isinstance(raw_results, dict):
            return list(raw_results.values())

        if isinstance(raw_results, list):
            return raw_results

        if isinstance(raw_results, tuple):
            return list(raw_results)

        if hasattr(raw_results, "values"):
            return list(raw_results.values())

        return list(raw_results)


class TaskGraphAnalyzer:
    # строим карты задач
    @staticmethod
    def build_task_map(tasks: list[TaskForRun]) -> dict[str, TaskForRun]:
        return {task.task_id: task for task in tasks}

    @staticmethod
    def build_success_result_map(results: list[TaskResult]) -> dict[str, TaskResult]:
        result_map = {}

        for result in results:
            if result.return_code == 0:
                result_map[task_id_of_result(result)] = result

        return result_map

    # построение потомков
    @staticmethod
    def compute_successors(tasks: list[TaskForRun]) -> dict[str, list[str]]:
        successors = {task.task_id: [] for task in tasks}

        for task in tasks:
            for dep in task.deps:
                if dep in successors:
                    successors[dep].append(task.task_id)

        return successors

    # считаем blevel для критического пути blevel(task) = duration(task) + максимум blevel среди потомков
    @staticmethod
    def compute_blevel(tasks: list[TaskForRun]) -> dict[str, float]:
        task_map = TaskGraphAnalyzer.build_task_map(tasks)
        successors = TaskGraphAnalyzer.compute_successors(tasks)
        blevel: dict[str, float] = {}

        def dfs(task_id: str) -> float:
            if task_id in blevel:
                return blevel[task_id]

            task = task_map[task_id]

            if not successors[task_id]:
                blevel[task_id] = task.duration
            else:
                blevel[task_id] = task.duration + max(dfs(child) for child in successors[task_id])

            return blevel[task_id]

        for task in tasks:
            dfs(task.task_id)

        return blevel

    # строим критический путь 
    @staticmethod
    def compute_critical_path(tasks: list[TaskForRun]) -> list[str]:
        blevel = TaskGraphAnalyzer.compute_blevel(tasks)
        successors = TaskGraphAnalyzer.compute_successors(tasks)

        start_tasks = [task.task_id for task in tasks if not task.deps]

        if not start_tasks:
            return []

        current = max(start_tasks, key=lambda task_id: blevel[task_id])
        path = [current]

        while successors[current]:
            current = max(successors[current], key=lambda task_id: blevel[task_id])
            path.append(current)

        return path


class ResultMetrics:
    def __init__(self, runner_count: int) -> None:
        self._runner_count = runner_count

    # считаем время готовности задачи к запуску
    def compute_ready_time(
        self,
        task: TaskForRun,
        result_map: dict[str, TaskResult],
        run_start_ts: float,
    ) -> float:
        if not task.deps:
            return run_start_ts

        dep_end_times = []

        for dep in task.deps:
            dep_result = result_map.get(dep)

            if dep_result is not None:
                dep_end_times.append(result_end_ts(dep_result))

        if not dep_end_times:
            return run_start_ts

        return max(dep_end_times)

    # расчет метрик
    def compute_scheduler_metrics(
        self,
        scheduler_name: str,
        tasks: list[TaskForRun],
        results: list[TaskResult],
        elapsed: float,
        sequential_time: float | None,
    ) -> dict[str, Any]:
        task_map = TaskGraphAnalyzer.build_task_map(tasks)
        result_map = TaskGraphAnalyzer.build_success_result_map(results)

        # фактическое время выполнения всего набора задач Cmax = max completion_time - min start_time потом его сравниваем между планировщиками 
        if result_map:
            run_start_ts = min(result_start_ts(result) for result in result_map.values())
            run_end_ts = max(result_end_ts(result) for result in result_map.values())
            measured_cmax = run_end_ts - run_start_ts
        else:
            run_start_ts = 0.0
            measured_cmax = elapsed

        # суммарная длительность всех задач
        total_work = sum(task.duration for task in tasks)
        actual_busy_time = 0.0
        waits = []

        for task_id, result in result_map.items():
            task = task_map.get(task_id)

            if task is None:
                continue

            # фактическое суммарное время работы всех задач по результатам запуска
            actual_busy_time += max(0.0, result_end_ts(result) - result_start_ts(result))

            ready_ts = self.compute_ready_time(task, result_map, run_start_ts)
            wait = max(0.0, result_start_ts(result) - ready_ts)
            waits.append(wait)

        # считаем среднее время ожидания задачи после ее готовности (планировщик не должен хранить готовые задачи слишком долго)
        avg_wait = sum(waits) / len(waits) if waits else 0.0

        blevel = TaskGraphAnalyzer.compute_blevel(tasks)
        critical_path = TaskGraphAnalyzer.compute_critical_path(tasks)
        critical_path_length = max(blevel.values()) if blevel else 0.0 # быстрее критического пути невозмоэно выполнить весь граф

        # нижняя оцекна для выполнения обхема работы (3 раннера - делим на 3)
        lower_bound_by_work = total_work / self._runner_count
        # общая нижняя оценка LB = max(Lcp, W / m)
        # Lcp — длина критического пути; W — суммарная работа;m - число раннеров.
        lower_bound = max(critical_path_length, lower_bound_by_work)

        # показывает во сколько раз текущий планирофшик быстрее послежовательного
        speedup = None
        #эффективность использования ресурсов от идеалного варианта
        efficiency = None

        if sequential_time is not None and measured_cmax > 0:
            speedup = sequential_time / measured_cmax
            efficiency = speedup / self._runner_count

        # средняя загрузка исполнителей
        utilization = 0.0

        if measured_cmax > 0:
            utilization = actual_busy_time / (self._runner_count * measured_cmax)

        # насколько результат далек от теоретического минимума времени
        gap_to_lower_bound = None

        if lower_bound > 0:
            gap_to_lower_bound = measured_cmax / lower_bound

        return {
            "scheduler": scheduler_name,
            "task_count": len(tasks),
            "runner_count": self._runner_count,
            "cmax_sec": measured_cmax,
            "elapsed_sec": elapsed,
            "total_work_sec": total_work,
            "critical_path_sec": critical_path_length,
            "lower_bound_sec": lower_bound,
            "speedup_vs_sequential": speedup,
            "efficiency": efficiency,
            "utilization": utilization,
            "avg_wait_sec": avg_wait,
            "gap_to_lower_bound": gap_to_lower_bound,
            "critical_path": " -> ".join(critical_path),
        }

    # для визуализации
    def make_trace_rows(
        self,
        scheduler_name: str,
        tasks: list[TaskForRun],
        results: list[TaskResult],
    ) -> list[dict[str, Any]]:
        task_map = TaskGraphAnalyzer.build_task_map(tasks)
        result_map = TaskGraphAnalyzer.build_success_result_map(results)

        if not result_map:
            return []

        run_start_ts = min(result_start_ts(result) for result in result_map.values())
        critical_path = set(TaskGraphAnalyzer.compute_critical_path(tasks))
        trace_rows = []

        for task_id, result in result_map.items():
            task = task_map.get(task_id)

            if task is None:
                continue

            start_rel = result_start_ts(result) - run_start_ts
            end_rel = result_end_ts(result) - run_start_ts
            ready_ts = self.compute_ready_time(task, result_map, run_start_ts)
            ready_rel = ready_ts - run_start_ts
            wait_sec = max(0.0, result_start_ts(result) - ready_ts)

            trace_rows.append(
                {
                    "scheduler": scheduler_name,
                    "task_id": task_id,
                    "priority": task.priority,
                    "duration": task.duration,
                    "deps": " ".join(task.deps),
                    "is_critical": int(task_id in critical_path),
                    "ready_sec": ready_rel,
                    "start_sec": start_rel,
                    "end_sec": end_rel,
                    "exec_sec": end_rel - start_rel,
                    "wait_sec": wait_sec,
                }
            )

        trace_rows.sort(key=lambda row: (row["scheduler"], row["start_sec"], row["task_id"]))
        return trace_rows


class CsvReportWriter:
    def __init__(self, config: MainConfig) -> None:
        self._config = config

    def save_metrics(self, metrics_rows: list[dict[str, Any]]) -> None:
        fieldnames = [
            "scheduler",
            "task_count",
            "runner_count",
            "cmax_sec",
            "elapsed_sec",
            "total_work_sec",
            "critical_path_sec",
            "lower_bound_sec",
            "speedup_vs_sequential",
            "efficiency",
            "utilization",
            "avg_wait_sec",
            "gap_to_lower_bound",
            "critical_path",
        ]

        with self._config.metrics_file.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(metrics_rows)

    def save_trace(self, trace_rows: list[dict[str, Any]]) -> None:
        fieldnames = [
            "scheduler",
            "task_id",
            "priority",
            "duration",
            "deps",
            "is_critical",
            "ready_sec",
            "start_sec",
            "end_sec",
            "exec_sec",
            "wait_sec",
        ]

        with self._config.trace_file.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(trace_rows)

    def save_summary(self, summary: list[dict[str, Any]]) -> None:
        fieldnames = [
            "scheduler",
            "task_count",
            "elapsed_sec",
            "speedup_vs_sequential",
            "tasks_done",
            "tasks_failed",
            "status",
            "comment",
        ]

        with self._config.results_file.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summary)


class SummaryPrinter:
    def __init__(self, config: MainConfig) -> None:
        self._config = config

    def print_summary(self, summary: list[dict[str, Any]]) -> None:
        print()
        print("Сравнение планировщиков")
        print("-" * 96)
        print(
            f"{'scheduler':<16} "
            f"{'tasks':>6} "
            f"{'elapsed_sec':>12} "
            f"{'speedup':>10} "
            f"{'done':>8} "
            f"{'failed':>8} "
            f"{'status':>10}"
        )
        print("-" * 96)

        for row in summary:
            elapsed = row["elapsed_sec"]
            speedup = row["speedup_vs_sequential"]

            elapsed_text = f"{elapsed:.3f}" if isinstance(elapsed, float) else "-"
            speedup_text = f"{speedup:.2f}x" if isinstance(speedup, float) else "-"

            print(
                f"{row['scheduler']:<16} "
                f"{row['task_count']:>6} "
                f"{elapsed_text:>12} "
                f"{speedup_text:>10} "
                f"{row['tasks_done']:>8} "
                f"{row['tasks_failed']:>8} "
                f"{row['status']:>10}"
            )

        print("-" * 96)
        print(f"CSV сохранён в: {self._config.results_file}")


class MainApplication:
    def __init__(self, config: MainConfig) -> None:
        self._config = config
        self._task_loader = TaskFileLoader(config)
        self._runner_builder = RunnerBuilder(config)
        self._metrics = ResultMetrics(config.runner_count)
        self._writer = CsvReportWriter(config)
        self._printer = SummaryPrinter(config)

    def run(self) -> None:
        task_rows = self._task_loader.load_task_rows()
        self._task_loader.ensure_task_scripts(task_rows)

        summary: list[dict[str, Any]] = []
        metrics_rows: list[dict[str, Any]] = []
        trace_rows: list[dict[str, Any]] = []

        sequential_time = None

        print(f"Файл задач: {self._task_loader.get_tasks_file()}")

        for scheduler_name, kind_name in self._config.schedulers_to_test:
            print(f"Запуск планировщика: {scheduler_name}")

            tasks = self._task_loader.build_tasks(task_rows)
            runners = self._runner_builder.create_runners()
            kind = SchedulerLauncher.resolve_scheduler_kind(kind_name)

            try:
                scheduler = SchedulerLauncher.create_scheduler(kind, runners, tasks)
                elapsed = SchedulerLauncher.run_scheduler(scheduler)

                results = SchedulerLauncher.collect_results(scheduler)
                tasks_done, tasks_failed = count_successful_results(results)

                if scheduler_name == "sequential":
                    sequential_time = self._compute_sequential_time(results, elapsed)

                metrics = self._metrics.compute_scheduler_metrics(
                    scheduler_name=scheduler_name,
                    tasks=tasks,
                    results=results,
                    elapsed=elapsed,
                    sequential_time=sequential_time,
                )

                speedup = metrics["speedup_vs_sequential"]

                summary.append(
                    {
                        "scheduler": scheduler_name,
                        "task_count": len(tasks),
                        "elapsed_sec": metrics["cmax_sec"],
                        "speedup_vs_sequential": speedup,
                        "tasks_done": tasks_done,
                        "tasks_failed": tasks_failed,
                        "status": "ok",
                        "comment": "",
                    }
                )

                metrics_rows.append(metrics)
                trace_rows.extend(self._metrics.make_trace_rows(scheduler_name, tasks, results))

            except Exception as exc:
                summary.append(
                    {
                        "scheduler": scheduler_name,
                        "task_count": len(tasks),
                        "elapsed_sec": "",
                        "speedup_vs_sequential": "",
                        "tasks_done": 0,
                        "tasks_failed": 0,
                        "status": "error",
                        "comment": str(exc),
                    }
                )

                print(f"Ошибка при запуске {scheduler_name}: {exc}")

        self._writer.save_summary(summary)
        self._writer.save_metrics(metrics_rows)
        self._writer.save_trace(trace_rows)
        self._printer.print_summary(summary)

        print(f"Метрики сохранены в: {self._config.metrics_file}")
        print(f"Трассировка задач сохранена в: {self._config.trace_file}")

    def _compute_sequential_time(self, results: list[TaskResult], elapsed: float) -> float:
        successful_result_map = TaskGraphAnalyzer.build_success_result_map(results)

        if successful_result_map:
            start_ts = min(result_start_ts(result) for result in successful_result_map.values())
            end_ts = max(result_end_ts(result) for result in successful_result_map.values())
            return end_ts - start_ts

        return elapsed


def count_successful_results(results: Iterable[TaskResult]) -> tuple[int, int]:
    done = 0
    failed = 0

    for result in results:
        if result.return_code == 0:
            done += 1
        else:
            failed += 1

    return done, failed


def task_id_of_result(result: TaskResult) -> str:
    return result.task_id


def result_start_ts(result: TaskResult) -> float:
    return float(result.start_ts)


def result_end_ts(result: TaskResult) -> float:
    return float(result.end_ts)
