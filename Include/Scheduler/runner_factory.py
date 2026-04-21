from __future__ import annotations

from typing import Callable, Dict, Optional

from Src.local_runner import LocalRunner
from Include.Scheduler.runner_engine import run_with_deps
from Include.Scheduler.IFaces.task_result import TaskResult
from Include.Scheduler.IFaces.task_spec import TaskSpec

#движок исполнителя, на входе путь до скрипта задачи, набор задач и исполнителей, шумы и функци для выбора готовой здаачи
EngineFn = Callable[..., Dict[str, TaskResult]]

#фабрика, делаем так, чтоюы про runner_engine знала только она, а не весь проект. 
class RunnerFactory:
    @staticmethod
    def create_local_runner(
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
    ) -> LocalRunner:
# локальный раннер, оценивает ресурсы на задачу, собирает результаты, может параллельно задачи запускать
        return LocalRunner(
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

# возвращает двидок выполнения задач (с зависимостями итп)  
    @staticmethod
    def create_dependency_engine() -> EngineFn:
        return run_with_deps
