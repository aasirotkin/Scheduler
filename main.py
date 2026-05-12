#логика main.py 
# 1. берем задачи с их параметрами и зависимостями из json
# 2. создаем скрипты задач и объектов задач для запуска
# 3. создаем локальные раннеры
# 4. создаем и запускаем планировщики
# 5. считаем метрики сравнения и создаем таблицу csv

# TODO: Лиза, в целом молодец. Папка Include уже ничего, там я написал что-то, но больше к ней не придираюсь.
# Объясню, зачем я вообще придираюсь. Мне кажется, что смотреть будут в коде два места.
# 1-ое это папку Include и 2-ое это main. Поэтому важно, чтобы всё было супер читаемое, не было
# ничего лишнего и ни что не коробило глаз.
# Сейчас main - это помойка (я не обзываюсь, просто так говорят).
# вот выше ты написала логику работы main. А теперь сделай так, чтобы можно было
# смотреть на код и без этого верхнего комментраия понять что тут происходит.
# Это называется декомпозицией кода. Разбей всё на функции / класс (вижу что и так есть),
# вынеси в отдельные файлы помощники.
# Итоговый код должен читаться как повествование, которое не требует объяснений.

from __future__ import annotations

import sys
from pathlib import Path

# определяем папку = корень проекта для импортов всех файлов 
ROOT_DIR = Path(__file__).resolve().parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# импорт класса-помощника. MainConfig — это объект с настройками запуска. MainApplication — это класс, внутри которого лежит вся основная логика
try:
    from Src.main_helper import MainApplication, MainConfig
except ImportError as exc:
    raise ImportError(
        "Не удалось импортировать MainApplication"
    ) from exc


# задаем основные файлы-константы для запуска
TASKS_FILE = ROOT_DIR / "Tests" / "tasks.json"
RESULTS_FILE = ROOT_DIR / "scheduler_compare.csv"
TRACE_FILE = ROOT_DIR / "task_trace.csv"
METRICS_FILE = ROOT_DIR / "scheduler_metrics.csv"
RUNNER_COUNT = 3

# список планировщиков для прогона, важно чтобы последовательный был первым, чтобы сразу на него опираться далее при сравнении
SCHEDULERS_TO_TEST = [
    ("sequential", "SEQUENTIAL"),
    ("parallel", "PARALLEL"),
    ("dag_priority", "DAG_PRIORITY"),
    ("dag_critical", "DAG_CRITICAL"),
]


# конфигурация запуска 
def build_config() -> MainConfig:
    return MainConfig(
        root_dir=ROOT_DIR,
        tasks_file=TASKS_FILE,
        results_file=RESULTS_FILE,
        trace_file=TRACE_FILE,
        metrics_file=METRICS_FILE,
        runner_count=RUNNER_COUNT,
        schedulers_to_test=SCHEDULERS_TO_TEST,
    )


# создаем приложение с конфигурацией, запускаем
def main() -> None:
    application = MainApplication(build_config())
    application.run()


if __name__ == "__main__":
    main()
