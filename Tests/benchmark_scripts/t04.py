#!/usr/bin/env python3

import time


TASK_ID = "t04"
DURATION = 0.8
CPU_PERCENT = 35.0
MEM_MB = 64.0


def main():
    # Небольшая имитация потребления памяти.
    # Фактическое выделение ограничено, чтобы тест не перегружал ноутбук.
    data = bytearray(int(min(MEM_MB, 32)) * 1024 * 1024)

    start = time.perf_counter()
    deadline = start + DURATION

    work_period = 0.05
    busy_part = work_period * max(0.0, min(CPU_PERCENT, 100.0)) / 100.0

    checksum = 0

    while time.perf_counter() < deadline:
        busy_deadline = time.perf_counter() + busy_part

        while time.perf_counter() < busy_deadline:
            checksum = (checksum * 31 + 7) % 1000003

        sleep_time = max(0.0, work_period - busy_part)

        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"task={TASK_ID} finished; checksum={checksum}; memory={len(data)}")


if __name__ == "__main__":
    main()
