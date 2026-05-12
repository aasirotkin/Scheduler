from __future__ import annotations

from typing import Any, Optional

from Src.runner_factory_helper import RunnerFactoryHelper


# фабрика раннеров, которая создает исполнителей задач
class RunnerFactory:
    @staticmethod
    def create_local_runner(
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
        return RunnerFactoryHelper.create_process_runner(
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
