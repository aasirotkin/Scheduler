from __future__ import annotations

from Src.local_runner import LocalRunner

# фабрика, делаем так, чтобы про runner_engine знала только она, а не весь проект
class RunnerFactory:
    @staticmethod
    def create_local_runner(
        runner_id: str,
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
        # локальный раннер, оценивает ресурсы на задачу,
        # собирает результаты, может параллельно запускать несколько задач
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


