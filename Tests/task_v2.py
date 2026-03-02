#!/usr/bin/env python3

#нагрузочная задача для экспериментальной части.

#Расширения:
#--task-id: для логов и детерминированного jitter.
#--jitter-pct: внутри задачи параметры слегка искажаются (обман планировщика), например 0.05..0.10.
#--seed: чтобы jitter был воспроизводимым между прогоном (в связке с task-id).


from __future__ import annotations

import argparse
import time
import math
import socket
import random

try:
    import psutil
except ImportError:
    psutil = None

#аргументы которые раннер передает в задачу
def parse_args():
    parser = argparse.ArgumentParser(
        description="Тестовая задача: память, CPU, сеть, время + метрики (если есть psutil)"
    )
    parser.add_argument("--task-id", type=str, default="", help="Id задачи (для логов/seed)")
    parser.add_argument("--mem-mb", type=float, required=True, help="Сколько МБ памяти выделить (оценка)")
    parser.add_argument("--net-mbps", type=float, required=True, help="Скорость генерации сети (Мбит/сек) (оценка)")
    parser.add_argument("--cpu-percent", type=float, required=True, help="Процентная загрузка CPU (0–100) (оценка)")
    parser.add_argument("--duration", type=float, required=True, help="Время, сек (оценка)")

    parser.add_argument("--jitter-pct", type=float, default=0.0,
                        help="Случайный разброс параметров внутри задачи, например 0.05..0.10")
    parser.add_argument("--seed", type=int, default=None, help="Seed для jitter (вместе с task-id)")

    return parser.parse_args()

#шумим
def _apply_jitter(rng: random.Random, x: float, jitter_pct: float, *, clamp_min: float | None = None, clamp_max: float | None = None) -> float:
    if jitter_pct <= 0:
        y = x
    else:
        k = rng.uniform(1.0 - jitter_pct, 1.0 + jitter_pct)
        y = x * k

    if clamp_min is not None:
        y = max(clamp_min, y)
    if clamp_max is not None:
        y = min(clamp_max, y)
    return y

#потребление памяти -- нагрузка
def allocate_memory(mem_mb: float):
    bytes_to_alloc = int(mem_mb * 1024 * 1024)
    print(f"Выделяем ~{mem_mb:.1f} МБ памяти ({bytes_to_alloc} байт)...")
    buf = bytearray(bytes_to_alloc)
    # прочек памяти: трогаем по странице
    for i in range(0, len(buf), 4096):
        buf[i] = 1
    return buf

#генерация сетевого трафика без реального сервера
def create_udp_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = ("127.0.0.1", 9999)
    return sock, target

#проверка входных данных и обман планировщика
def run_task(task_id: str, mem_mb: float, net_mbps: float, cpu_percent: float, duration: float, *, jitter_pct: float, seed: int | None):
    if cpu_percent < 0 or cpu_percent > 100:
        raise ValueError("cpu_percent должен быть в диапазоне [0, 100].")
    if net_mbps < 0:
        raise ValueError("net_mbps должен быть >= 0.")
    if mem_mb < 0:
        raise ValueError("mem_mb должен быть >= 0.")
    if duration < 0:
        raise ValueError("duration должен быть >= 0.")

    # jitter: обманка -- реальные параметры немного отличаются
    if jitter_pct and jitter_pct > 0:
    #Делает jitter воспроизводимым: одна и та же (seed, task_id) -> те же параметры
        rng = random.Random(f"{seed}:{task_id}") if seed is not None else random.Random()
        mem_mb = _apply_jitter(rng, mem_mb, jitter_pct, clamp_min=0.0)
        net_mbps = _apply_jitter(rng, net_mbps, jitter_pct, clamp_min=0.0)
        cpu_percent = _apply_jitter(rng, cpu_percent, jitter_pct, clamp_min=0.0, clamp_max=100.0)
        duration = _apply_jitter(rng, duration, jitter_pct, clamp_min=0.0)

    print("\n--- TASK START ---")
    if task_id:
        print(f"Task id: {task_id}")
    if jitter_pct and jitter_pct > 0:
        print(f"Jitter enabled: ±{jitter_pct * 100:.1f}% (seed={'none' if seed is None else seed})")

    print(f"Params (actual): mem≈{mem_mb:.1f} MB | net≈{net_mbps:.1f} Mbps | cpu≈{cpu_percent:.1f}% | duration≈{duration:.2f} s")


#подготовка нагрузки(память сеть метрики)
    buf = allocate_memory(mem_mb)

    sock, target = create_udp_socket()
    payload = b"x" * 1400  # UDP пакет ~1400 байт
    payload_len = len(payload)

    # сколько байт/сек нужно отправлять:
    bytes_per_sec_target = net_mbps * 1_000_000 / 8.0  # Мбит/с -> байт/с

    if psutil:
        psutil.cpu_percent(interval=None)  # сброс внутренней статистики
        net0 = psutil.net_io_counters()
        if net0 is None:
            prev_bytes_sent = prev_bytes_recv = None
        else:
            prev_bytes_sent = net0.bytes_sent
            prev_bytes_recv = net0.bytes_recv
    else:
        prev_bytes_sent = prev_bytes_recv = None


#ЦИКЛ НАГРУЗКИ 
    start_time = time.time()
    end_time = start_time + duration

    tick = 0.1  #базовый шаг цикла
    last_metrics_print = -1

    while True:
        now = time.time()
        if now >= end_time:
            break

        elapsed = now - start_time

        # вывод метрик раз в секунду
        if psutil and int(elapsed) != last_metrics_print:
            last_metrics_print = int(elapsed)
            cpu_now = psutil.cpu_percent(interval=None)
            vm = psutil.virtual_memory()
            net = psutil.net_io_counters()

            if net is None or prev_bytes_sent is None:
                tx_mbps = rx_mbps = 0.0
            else:
                delta_sent = net.bytes_sent - prev_bytes_sent
                delta_recv = net.bytes_recv - prev_bytes_recv
                prev_bytes_sent = net.bytes_sent
                prev_bytes_recv = net.bytes_recv
                tx_mbps = (delta_sent * 8) / 1_000_000
                rx_mbps = (delta_recv * 8) / 1_000_000

            print(
                f"[METRICS t={elapsed:5.1f}s] "
                f"CPU: {cpu_now:5.1f}% | "
                f"RAM used: {vm.used / (1024*1024):8.1f} MB ({vm.percent:4.1f}%) | "
                f"NET tx≈{tx_mbps:6.2f} Mbps, rx≈{rx_mbps:6.2f} Mbps"
            )

        tick_start = time.time()

        # CPU: busy-loop часть tick
        busy_time = tick * (cpu_percent / 100.0)
        busy_end = tick_start + busy_time
        x = 0.0
        while time.time() < busy_end:
            x += math.sqrt(823.896)  # бессмысленная математика =)

        # NET: отправляем N UDP пакетов за tick
        if net_mbps > 0:
            bytes_per_tick = bytes_per_sec_target * tick
            packets = int(bytes_per_tick / payload_len)
            for _ in range(packets):
                try:
                    sock.sendto(payload, target)
                except OSError:
                    break

        tick_elapsed = time.time() - tick_start
        sleep_time = tick - tick_elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    # держим buf до конца функции (иначе память может освободиться раньше)
    _ = buf

    print("--- TASK END ---\n")


def main():
    args = parse_args()
    run_task(
        task_id=args.task_id,
        mem_mb=args.mem_mb,
        net_mbps=args.net_mbps,
        cpu_percent=args.cpu_percent,
        duration=args.duration,
        jitter_pct=args.jitter_pct,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
