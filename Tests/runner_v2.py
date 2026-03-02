#!/usr/bin/env python3

from __future__ import annotations

import argparse

from runner_engine import load_tasks, save_csv
from schedulers_registry import SCHEDULERS


def main():
    ap = argparse.ArgumentParser(description="Runner for task.py: sequential / parallel / dag_priority / dag_critical")
    ap.add_argument("--tasks", required=True, help="Path to tasks.json")
    ap.add_argument("--task-py", default="task.py", help="Path to task.py")
    ap.add_argument("--scheduler", required=True, choices=sorted(SCHEDULERS.keys()),
                    help="Which scheduler to use")
    ap.add_argument("--workers", type=int, default=2, help="Parallel workers (ignored for sequential)")
    ap.add_argument("--out", default="run_results.csv", help="Output CSV")
    ap.add_argument("--jitter-pct", type=float, default=0.0, help="Random jitter inside task.py, e.g. 0.05..0.10")
    ap.add_argument("--seed", type=int, default=None, help="Seed for jitter reproducibility")
    args = ap.parse_args()

    tasks = load_tasks(args.tasks)
    sched = SCHEDULERS[args.scheduler]

    results = sched.run(
        args.task_py,
        tasks,
        workers=args.workers,
        jitter_pct=args.jitter_pct,
        seed=args.seed,
    )
    save_csv(args.out, results)


if __name__ == "__main__":
    main()
