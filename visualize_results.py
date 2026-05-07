#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch


ROOT_DIR = Path(__file__).resolve().parent

TASKS_FILE = ROOT_DIR / "Tests" / "benchmark_tasks.json"
METRICS_FILE = ROOT_DIR / "benchmark_metrics.csv"
TRACE_FILE = ROOT_DIR / "benchmark_task_trace.csv"
PLOTS_DIR = ROOT_DIR / "plots"


SCHEDULER_ORDER = [
    "sequential",
    "parallel",
    "dag_priority",
    "dag_critical",
]


SCHEDULER_TITLES = {
    "sequential": "Sequential",
    "parallel": "Parallel",
    "dag_priority": "DAG Priority",
    "dag_critical": "DAG Critical Path",
}


# Фиксированная палитра для задач.
# Цвет привязывается к task_id и сохраняется одинаковым на всех графиках.
TASK_PALETTE = [
    "#4E79A7",
    "#F28E2B",
    "#E15759",
    "#76B7B2",
    "#59A14F",
    "#EDC948",
    "#B07AA1",
    "#FF9DA7",
    "#9C755F",
    "#BAB0AC",
    "#86BCB6",
    "#FABFD2",
    "#8CD17D",
    "#B6992D",
    "#499894",
    "#D37295",
    "#A0CBE8",
    "#FFBE7D",
    "#D4A6C8",
    "#79706E",
]


def setup_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "#FAFAFA",
            "axes.edgecolor": "#333333",
            "axes.linewidth": 0.9,
            "axes.grid": True,
            "grid.color": "#D9D9D9",
            "grid.linewidth": 0.7,
            "grid.alpha": 0.55,
            "font.size": 10,
            "axes.titlesize": 14,
            "axes.titleweight": "bold",
            "axes.labelsize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.frameon": True,
            "legend.framealpha": 0.95,
            "legend.edgecolor": "#CCCCCC",
        }
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл: {path}")

    with path.open("r", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None

    return float(value)


def scheduler_title(name: str) -> str:
    return SCHEDULER_TITLES.get(name, name)


def ordered(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(
        rows,
        key=lambda row: SCHEDULER_ORDER.index(row["scheduler"])
        if row["scheduler"] in SCHEDULER_ORDER
        else 999,
    )


def load_tasks() -> tuple[list[dict], dict[str, dict]]:
    with TASKS_FILE.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, dict) and "tasks" in data:
        tasks = data["tasks"]
    elif isinstance(data, list):
        tasks = data
    else:
        raise ValueError("Неверный формат benchmark_tasks.json")

    normalized = []

    for task in tasks:
        item = dict(task)
        item["id"] = str(item["id"])
        item["deps"] = [str(dep) for dep in item.get("deps", [])]
        item["duration"] = float(item["duration"])
        item["priority"] = int(item["priority"])
        normalized.append(item)

    return normalized, {task["id"]: task for task in normalized}


def make_task_colors(tasks: list[dict]) -> dict[str, str]:
    task_ids = sorted(tasks, key=lambda task: task["id"])

    colors = {}

    for index, task in enumerate(task_ids):
        colors[task["id"]] = TASK_PALETTE[index % len(TASK_PALETTE)]

    return colors


def build_graph(tasks_by_id: dict[str, dict]) -> tuple[dict[str, list[str]], dict[str, int]]:
    successors = {task_id: [] for task_id in tasks_by_id}
    indegree = {task_id: 0 for task_id in tasks_by_id}

    for task_id, task in tasks_by_id.items():
        for dep in task["deps"]:
            if dep not in tasks_by_id:
                continue

            successors[dep].append(task_id)
            indegree[task_id] += 1

    for task_id in successors:
        successors[task_id].sort()

    return successors, indegree


def topological_order(tasks_by_id: dict[str, dict]) -> list[str]:
    successors, indegree = build_graph(tasks_by_id)
    queue = deque(sorted([task_id for task_id, degree in indegree.items() if degree == 0]))

    order = []

    while queue:
        current = queue.popleft()
        order.append(current)

        for child in successors[current]:
            indegree[child] -= 1

            if indegree[child] == 0:
                queue.append(child)

    if len(order) != len(tasks_by_id):
        raise ValueError("В графе есть цикл, DAG-визуализацию построить нельзя.")

    return order


def compute_levels(tasks_by_id: dict[str, dict]) -> dict[str, int]:
    successors, _ = build_graph(tasks_by_id)
    order = topological_order(tasks_by_id)

    levels = {task_id: 0 for task_id in tasks_by_id}

    for task_id in order:
        for child in successors[task_id]:
            levels[child] = max(levels[child], levels[task_id] + 1)

    return levels


def compute_blevel(tasks_by_id: dict[str, dict]) -> dict[str, float]:
    successors, _ = build_graph(tasks_by_id)
    order = topological_order(tasks_by_id)

    blevel = {}

    for task_id in reversed(order):
        duration = float(tasks_by_id[task_id]["duration"])

        if not successors[task_id]:
            blevel[task_id] = duration
        else:
            blevel[task_id] = duration + max(blevel[child] for child in successors[task_id])

    return blevel


def compute_critical_path(tasks_by_id: dict[str, dict]) -> list[str]:
    successors, indegree = build_graph(tasks_by_id)
    blevel = compute_blevel(tasks_by_id)

    starts = [task_id for task_id, degree in indegree.items() if degree == 0]

    if not starts:
        return []

    current = max(starts, key=lambda task_id: blevel[task_id])
    path = [current]

    while successors[current]:
        current = max(successors[current], key=lambda task_id: blevel[task_id])
        path.append(current)

    return path


def plot_metric_bar(
    rows: list[dict[str, str]],
    metric: str,
    title: str,
    ylabel: str,
    filename: str,
    value_format: str = "{:.3f}",
) -> None:
    rows = ordered(rows)

    names = []
    values = []

    for row in rows:
        value = to_float(row.get(metric))

        if value is None:
            continue

        names.append(scheduler_title(row["scheduler"]))
        values.append(value)

    if not values:
        return

    plt.figure(figsize=(10, 6))
    bars = plt.bar(names, values, color="#4E79A7", edgecolor="#333333", linewidth=1.1)

    plt.title(title)
    plt.xlabel("Планировщик")
    plt.ylabel(ylabel)
    plt.grid(axis="y")
    plt.grid(axis="x", visible=False)

    max_value = max(values)

    for bar, value in zip(bars, values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_value * 0.015,
            value_format.format(value),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.ylim(0, max_value * 1.15)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=220)
    plt.close()


def plot_cmax_with_global_bounds(rows: list[dict[str, str]]) -> None:
    rows = ordered(rows)

    names = []
    cmax_values = []

    for row in rows:
        cmax = to_float(row.get("cmax_sec"))

        if cmax is None:
            continue

        names.append(scheduler_title(row["scheduler"]))
        cmax_values.append(cmax)

    if not cmax_values:
        return

    # Lcp и LB одинаковые для всех строк, потому что считаются по одному входному DAG.
    first_row = rows[0]
    lcp = float(first_row["critical_path_sec"])
    lb = float(first_row["lower_bound_sec"])

    plt.figure(figsize=(11, 6))
    bars = plt.bar(
        names,
        cmax_values,
        color="#76B7B2",
        edgecolor="#333333",
        linewidth=1.1,
        label="Фактический Cmax",
    )

    plt.axhline(
        lb,
        color="#E15759",
        linestyle="--",
        linewidth=2.0,
        label=f"LB = max(Lcp, W/m) = {lb:.2f} c",
    )

    plt.axhline(
        lcp,
        color="#F28E2B",
        linestyle=":",
        linewidth=2.3,
        label=f"Lcp = {lcp:.2f} c",
    )

    max_value = max(max(cmax_values), lb, lcp)

    for bar, value in zip(bars, cmax_values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_value * 0.015,
            f"{value:.3f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.title("Makespan и нижние оценки математической модели")
    plt.xlabel("Планировщик")
    plt.ylabel("Время, с")
    plt.grid(axis="y")
    plt.grid(axis="x", visible=False)
    plt.ylim(0, max_value * 1.18)
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "cmax_with_bounds.png", dpi=220)
    plt.close()


def plot_gantt(
    trace_rows: list[dict[str, str]],
    scheduler_name: str,
    task_colors: dict[str, str],
    critical_path: list[str],
) -> None:
    rows = [row for row in trace_rows if row["scheduler"] == scheduler_name]

    if not rows:
        return

    rows.sort(key=lambda row: (float(row["start_sec"]), float(row["end_sec"]), row["task_id"]))

    critical_set = set(critical_path)

    plt.figure(figsize=(15, 8))

    y_positions = list(range(len(rows)))
    task_ids = [row["task_id"] for row in rows]

    for y, row in zip(y_positions, rows):
        task_id = row["task_id"]
        start = float(row["start_sec"])
        end = float(row["end_sec"])
        duration = end - start
        color = task_colors.get(task_id, "#CCCCCC")

        # ВАЖНО: толщина рамки одинаковая для всех задач.
        plt.barh(
            y,
            duration,
            left=start,
            color=color,
            edgecolor="#222222",
            linewidth=1.15,
            alpha=0.88,
        )

        label = f"{task_id}\np={row['priority']}"

        if task_id in critical_set:
            label = f"★ {label}"

        plt.text(
            start + duration / 2,
            y,
            label,
            ha="center",
            va="center",
            fontsize=8,
            color="#111111",
        )

    plt.yticks(y_positions, task_ids)
    plt.gca().invert_yaxis()

    plt.title(f"Gantt-диаграмма выполнения задач — {scheduler_title(scheduler_name)}")
    plt.xlabel("Время от начала запуска, с")
    plt.ylabel("Задача")
    plt.grid(axis="x")
    plt.grid(axis="y", visible=False)

    legend_elements = [
        Patch(facecolor="#FFFFFF", edgecolor="#222222", linewidth=1.15, label="Обычная задача"),
        Patch(facecolor="#FFFFFF", edgecolor="#222222", linewidth=1.15, label="★ задача критического пути"),
    ]

    plt.legend(handles=legend_elements, loc="lower right")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / f"gantt_{scheduler_name}.png", dpi=220)
    plt.close()


def plot_dag_structure(
    tasks_by_id: dict[str, dict],
    task_colors: dict[str, str],
    critical_path: list[str],
) -> None:
    successors, _ = build_graph(tasks_by_id)
    levels = compute_levels(tasks_by_id)
    blevel = compute_blevel(tasks_by_id)

    level_groups = defaultdict(list)

    for task_id, level in levels.items():
        level_groups[level].append(task_id)

    for level in level_groups:
        level_groups[level].sort()

    coords = {}

    for level, task_ids in level_groups.items():
        count = len(task_ids)

        for index, task_id in enumerate(task_ids):
            x = level
            y = -(index - (count - 1) / 2)
            coords[task_id] = (x, y)

    critical_set = set(critical_path)
    critical_edges = set(zip(critical_path[:-1], critical_path[1:]))

    plt.figure(figsize=(16, 9))

    for src, children in successors.items():
        x1, y1 = coords[src]

        for dst in children:
            x2, y2 = coords[dst]
            is_critical_edge = (src, dst) in critical_edges

            plt.annotate(
                "",
                xy=(x2 - 0.12, y2),
                xytext=(x1 + 0.12, y1),
                arrowprops=dict(
                    arrowstyle="->",
                    color="#333333" if is_critical_edge else "#9E9E9E",
                    lw=2.0 if is_critical_edge else 1.0,
                    alpha=0.9 if is_critical_edge else 0.55,
                ),
            )

    for task_id, task in tasks_by_id.items():
        x, y = coords[task_id]
        color = task_colors.get(task_id, "#CCCCCC")

        # ВАЖНО: рамки задач одинаковой толщины.
        plt.scatter(
            [x],
            [y],
            s=1700,
            color=color,
            edgecolor="#222222",
            linewidth=1.15,
            alpha=0.92,
            zorder=3,
        )

        star = "★ " if task_id in critical_set else ""

        label = (
            f"{star}{task_id}\n"
            f"d={task['duration']:.1f}s\n"
            f"p={task['priority']}\n"
            f"b={blevel[task_id]:.1f}"
        )

        plt.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            fontsize=8,
            color="#111111",
            zorder=4,
        )

    max_level = max(level_groups.keys()) if level_groups else 0

    plt.title("DAG тестового набора задач")
    plt.xlabel("Уровень DAG")
    plt.ylabel("")
    plt.xticks(range(max_level + 1))
    plt.yticks([])
    plt.grid(axis="x")
    plt.grid(axis="y", visible=False)

    legend_elements = [
        Patch(facecolor="#FFFFFF", edgecolor="#222222", linewidth=1.15, label="Обычная задача"),
        Patch(facecolor="#FFFFFF", edgecolor="#222222", linewidth=1.15, label="★ задача критического пути"),
    ]

    plt.legend(handles=legend_elements, loc="upper right")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "dag_structure.png", dpi=220)
    plt.close()

def plot_gantt_all(
    trace_rows: list[dict[str, str]],
    task_colors: dict[str, str],
    critical_path: list[str],
) -> None:
    critical_set = set(critical_path)

    scheduler_rows = {}

    for scheduler in SCHEDULER_ORDER:
        rows = [row for row in trace_rows if row["scheduler"] == scheduler]

        if not rows:
            continue

        rows.sort(
            key=lambda row: (
                float(row["start_sec"]),
                float(row["end_sec"]),
                row["task_id"],
            )
        )

        scheduler_rows[scheduler] = rows

    if not scheduler_rows:
        return

    max_end = 0.0

    for rows in scheduler_rows.values():
        for row in rows:
            max_end = max(max_end, float(row["end_sec"]))

    fig_height = max(10, 3.4 * len(scheduler_rows))

    fig, axes = plt.subplots(
        nrows=len(scheduler_rows),
        ncols=1,
        figsize=(16, fig_height),
        sharex=True,
    )

    if len(scheduler_rows) == 1:
        axes = [axes]

    for ax, scheduler in zip(axes, scheduler_rows.keys()):
        rows = scheduler_rows[scheduler]

        y_positions = list(range(len(rows)))
        task_ids = [row["task_id"] for row in rows]

        for y, row in zip(y_positions, rows):
            task_id = row["task_id"]
            start = float(row["start_sec"])
            end = float(row["end_sec"])
            duration = end - start

            color = task_colors.get(task_id, "#CCCCCC")

            ax.barh(
                y,
                duration,
                left=start,
                color=color,
                edgecolor="#222222",
                linewidth=1.15,
                alpha=0.88,
            )

            label = f"{task_id}"

            if task_id in critical_set:
                label = f"★ {label}"

            ax.text(
                start + duration / 2,
                y,
                label,
                ha="center",
                va="center",
                fontsize=8,
                color="#111111",
            )

        ax.set_yticks(y_positions)
        ax.set_yticklabels(task_ids)
        ax.invert_yaxis()

        ax.set_title(scheduler_title(scheduler), loc="left", fontsize=12, fontweight="bold")
        ax.set_ylabel("Задачи")
        ax.grid(axis="x")
        ax.grid(axis="y", visible=False)

        ax.set_xlim(0, max_end * 1.05)

    axes[-1].set_xlabel("Время от начала запуска, с")

    fig.suptitle(
        "Сравнение расписаний выполнения задач по планировщикам",
        fontsize=16,
        fontweight="bold",
        y=0.995,
    )

    legend_elements = [
        Patch(
            facecolor="#FFFFFF",
            edgecolor="#222222",
            linewidth=1.15,
            label="Обычная задача",
        ),
        Patch(
            facecolor="#FFFFFF",
            edgecolor="#222222",
            linewidth=1.15,
            label="★ задача критического пути",
        ),
    ]

    fig.legend(
        handles=legend_elements,
        loc="lower center",
        ncol=2,
        frameon=True,
        bbox_to_anchor=(0.5, 0.01),
    )

    plt.tight_layout(rect=[0, 0.04, 1, 0.97])
    plt.savefig(PLOTS_DIR / "gantt_all_schedulers.png", dpi=220)
    plt.close()


def plot_wait_by_scheduler(trace_rows: list[dict[str, str]]) -> None:
    grouped = defaultdict(list)

    for row in trace_rows:
        grouped[row["scheduler"]].append(float(row["wait_sec"]))

    schedulers = [name for name in SCHEDULER_ORDER if name in grouped]
    values = [sum(grouped[name]) / len(grouped[name]) for name in schedulers]
    labels = [scheduler_title(name) for name in schedulers]

    if not values:
        return

    plt.figure(figsize=(10, 6))
    bars = plt.bar(labels, values, color="#B07AA1", edgecolor="#333333", linewidth=1.1)

    plt.title("Среднее ожидание задач после готовности")
    plt.xlabel("Планировщик")
    plt.ylabel("Среднее ожидание, с")
    plt.grid(axis="y")
    plt.grid(axis="x", visible=False)

    max_value = max(values)

    for bar, value in zip(bars, values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_value * 0.015,
            f"{value:.3f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.ylim(0, max_value * 1.18 if max_value > 0 else 1)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "avg_wait_comparison.png", dpi=220)
    plt.close()


def plot_critical_vs_noncritical_wait(
    trace_rows: list[dict[str, str]],
    critical_path: list[str],
) -> None:
    critical_set = set(critical_path)
    data = defaultdict(lambda: {"critical": [], "noncritical": []})

    for row in trace_rows:
        scheduler = row["scheduler"]
        task_id = row["task_id"]
        wait = float(row["wait_sec"])

        if task_id in critical_set:
            data[scheduler]["critical"].append(wait)
        else:
            data[scheduler]["noncritical"].append(wait)

    schedulers = [name for name in SCHEDULER_ORDER if name in data]

    critical_values = []
    noncritical_values = []

    for scheduler in schedulers:
        critical = data[scheduler]["critical"]
        noncritical = data[scheduler]["noncritical"]

        critical_values.append(sum(critical) / len(critical) if critical else 0.0)
        noncritical_values.append(sum(noncritical) / len(noncritical) if noncritical else 0.0)

    labels = [scheduler_title(name) for name in schedulers]
    x = list(range(len(labels)))
    width = 0.36

    plt.figure(figsize=(12, 6))

    plt.bar(
        [i - width / 2 for i in x],
        critical_values,
        width=width,
        color="#E15759",
        edgecolor="#333333",
        linewidth=1.1,
        label="Критический путь",
    )

    plt.bar(
        [i + width / 2 for i in x],
        noncritical_values,
        width=width,
        color="#4E79A7",
        edgecolor="#333333",
        linewidth=1.1,
        label="Остальные задачи",
    )

    plt.xticks(x, labels)
    plt.title("Ожидание задач критического пути и остальных задач")
    plt.xlabel("Планировщик")
    plt.ylabel("Среднее ожидание, с")
    plt.grid(axis="y")
    plt.grid(axis="x", visible=False)
    plt.legend()

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "critical_vs_noncritical_wait.png", dpi=220)
    plt.close()


def plot_task_durations(
    tasks: list[dict],
    task_colors: dict[str, str],
    critical_path: list[str],
) -> None:
    rows = sorted(tasks, key=lambda task: task["duration"], reverse=True)
    critical_set = set(critical_path)

    names = [task["id"] for task in rows]
    durations = [task["duration"] for task in rows]
    colors = [task_colors[task["id"]] for task in rows]

    plt.figure(figsize=(12, 8))
    bars = plt.barh(
        names,
        durations,
        color=colors,
        edgecolor="#333333",
        linewidth=1.1,
        alpha=0.9,
    )

    plt.gca().invert_yaxis()
    plt.title("Длительности задач тестового набора")
    plt.xlabel("Длительность, с")
    plt.ylabel("Задача")
    plt.grid(axis="x")
    plt.grid(axis="y", visible=False)

    max_value = max(durations)

    for bar, task in zip(bars, rows):
        task_id = task["id"]
        text = f"{task['duration']:.1f}s, p={task['priority']}"

        if task_id in critical_set:
            text = "★ " + text

        plt.text(
            task["duration"] + max_value * 0.01,
            bar.get_y() + bar.get_height() / 2,
            text,
            va="center",
            fontsize=9,
        )

    plt.xlim(0, max_value * 1.25)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "task_durations.png", dpi=220)
    plt.close()


def plot_task_color_legend(
    tasks: list[dict],
    task_colors: dict[str, str],
    critical_path: list[str],
) -> None:
    critical_set = set(critical_path)
    rows = sorted(tasks, key=lambda task: task["id"])

    fig_height = max(5, len(rows) * 0.35)
    plt.figure(figsize=(8, fig_height))

    for index, task in enumerate(rows):
        task_id = task["id"]
        y = len(rows) - index

        plt.scatter(
            [0],
            [y],
            s=380,
            color=task_colors[task_id],
            edgecolor="#222222",
            linewidth=1.15,
        )

        star = "★ " if task_id in critical_set else ""
        label = (
            f"{star}{task_id}: "
            f"duration={task['duration']:.1f}s, "
            f"priority={task['priority']}, "
            f"deps={task['deps']}"
        )

        plt.text(0.15, y, label, va="center", fontsize=9)

    plt.title("Легенда цветов задач")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "task_color_legend.png", dpi=220)
    plt.close()


def main() -> None:
    setup_style()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    tasks, tasks_by_id = load_tasks()
    task_colors = make_task_colors(tasks)
    critical_path = compute_critical_path(tasks_by_id)

    metrics_rows = read_csv(METRICS_FILE)
    trace_rows = read_csv(TRACE_FILE)

    plot_metric_bar(
        rows=metrics_rows,
        metric="cmax_sec",
        title="Makespan планировщиков",
        ylabel="Cmax, с",
        filename="cmax_comparison.png",
    )

    plot_metric_bar(
        rows=metrics_rows,
        metric="speedup_vs_sequential",
        title="Ускорение относительно Sequential",
        ylabel="Speedup, x",
        filename="speedup_comparison.png",
        value_format="{:.2f}x",
    )

    plot_metric_bar(
        rows=metrics_rows,
        metric="utilization",
        title="Средняя загрузка исполнителей",
        ylabel="Utilization",
        filename="utilization_comparison.png",
        value_format="{:.2f}",
    )

    plot_metric_bar(
        rows=metrics_rows,
        metric="gap_to_lower_bound",
        title="Отклонение от нижней оценки",
        ylabel="Cmax / LB",
        filename="gap_to_lower_bound.png",
        value_format="{:.2f}",
    )

    plot_cmax_with_global_bounds(metrics_rows)
    plot_wait_by_scheduler(trace_rows)
    plot_critical_vs_noncritical_wait(trace_rows, critical_path)
    plot_dag_structure(tasks_by_id, task_colors, critical_path)
    plot_task_durations(tasks, task_colors, critical_path)

    for scheduler in SCHEDULER_ORDER:
        plot_gantt(trace_rows, scheduler, task_colors, critical_path)

    plot_gantt_all(trace_rows, task_colors, critical_path)


    print("Критический путь:")
    print(" -> ".join(critical_path))
    print()
    print(f"Графики сохранены в папку: {PLOTS_DIR}")


if __name__ == "__main__":
    main()
