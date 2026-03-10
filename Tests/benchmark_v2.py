#!/usr/bin/env python3
"""
benchmark_v2.py — прогон нескольких наборов задач на нескольких планировщиках
и генерация сводной таблицы (CSV).
"""

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from external_process_task import ExternalProcessTask
from scheduler_factory import create_scheduler


ALLOWED_SCHEDULERS = ("sequential", "parallel", "dag_priority", "dag_critical")


@dataclass
class RunRow:
    tasks_file: str
    scheduler: str
    workers: int
    jitter_pct: float
    seed: str
    makespan_s: float
    speedup_vs_seq: float


def compute_makespan(results: Dict[str, object]) -> float:
    starts: List[float] = []
    ends: List[float] = []
    for r in results.values():
        starts.append(float(getattr(r, "start_ts")))
        ends.append(float(getattr(r, "end_ts")))
    return max(ends) - min(starts)


def main():
    ap = argparse.ArgumentParser(description="Benchmark runner_v2 schedulers and build summary table")
    ap.add_argument("--runner", default="runner_v2.py", help="Path to runner_v2.py")
    ap.add_argument("--task", default="task_v2.py", help="Path to task_v2.py")
    ap.add_argument("--tasks", nargs="+", required=True, help="List of tasks*.json files")
    ap.add_argument("--schedulers", nargs="+", required=True, help="Schedulers list, e.g. sequential parallel dag_priority dag_critical")
    ap.add_argument("--workers", type=int, default=2, help="workers for non-sequential schedulers")
    ap.add_argument("--jitter-pct", type=float, default=0.0, help="jitter inside task.py")
    ap.add_argument("--seed", type=int, default=None, help="seed for jitter")
    ap.add_argument("--out", default="summary.csv", help="output summary CSV")
    ap.add_argument("--keep-csv", action="store_true", help="keep per-run CSVs")
    args = ap.parse_args()

    runner_dir = Path(args.runner).resolve().parent
    if str(runner_dir) not in sys.path:
        sys.path.insert(0, str(runner_dir))

    from runner_engine import load_tasks, save_csv

    rows: List[RunRow] = []

    for tasks_file in args.tasks:
        tasks_name = Path(tasks_file).stem
        tasks = load_tasks(tasks_file)

        per_case_rows: List[RunRow] = []
        makespan_by_sched: Dict[str, float] = {}

        for sched_name in args.schedulers:
            if sched_name not in ALLOWED_SCHEDULERS:
                raise ValueError(f"Unknown scheduler: {sched_name}. Allowed: {sorted(ALLOWED_SCHEDULERS)}")

            out_csv = f"run_{tasks_name}__{sched_name}.csv"
            workers_used = 1 if sched_name == "sequential" else args.workers

            sched = create_scheduler(
                kind=sched_name,
                workers=workers_used,
                seed=args.seed,
                jitter_pct=args.jitter_pct,
            )

            for spec in tasks.values():
                sched.add_task(ExternalProcessTask(spec=spec, entrypoint=args.task))

            results = sched.run_all()
            save_csv(out_csv, results)

            ms = compute_makespan(results)
            makespan_by_sched[sched_name] = ms

            per_case_rows.append(
                RunRow(
                    tasks_file=tasks_file,
                    scheduler=sched_name,
                    workers=workers_used,
                    jitter_pct=args.jitter_pct,
                    seed=str(args.seed) if args.seed is not None else "",
                    makespan_s=ms,
                    speedup_vs_seq=0.0,
                )
            )

        makespan_seq = makespan_by_sched.get("sequential")
        for row in per_case_rows:
            if makespan_seq and row.makespan_s > 0:
                row.speedup_vs_seq = makespan_seq / row.makespan_s
            else:
                row.speedup_vs_seq = 0.0

        rows.extend(per_case_rows)

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tasks_file", "scheduler", "workers", "jitter_pct", "seed", "makespan_s", "speedup_vs_seq"])
        for row in rows:
            w.writerow([
                row.tasks_file,
                row.scheduler,
                row.workers,
                row.jitter_pct,
                row.seed,
                f"{row.makespan_s:.6f}",
                f"{row.speedup_vs_seq:.3f}",
            ])

    print(f"[OK] summary saved: {args.out}")


if __name__ == "__main__":
    main()
