# -*- coding: utf-8 -*-
"""Utilitário de profiling de recursos (CPU/memória/tempo) por bloco de código."""

import threading
import time
from contextlib import contextmanager

import psutil

# Intervalo (segundos) entre amostras do profiler
PROFILE_SAMPLE_INTERVAL = 0.5


@contextmanager
def profile_resources(label: str):
    """
    Context manager que mede consumo de recursos do processo durante um bloco.

    Amostra RSS e CPU em uma thread de fundo e imprime um relatório ao final.

    Args:
        label (str): Identificador do bloco medido (aparece no relatório).
    """
    process = psutil.Process()
    process.cpu_percent(None)  # primeira chamada inicializa a medição

    samples: list[tuple[float, float]] = []  # (rss_bytes, cpu_percent)
    stop = threading.Event()

    def _sampler():
        first = True
        while not stop.is_set():
            try:
                rss = process.memory_info().rss
                cpu = process.cpu_percent(None)
            except psutil.Error:
                break
            # Primeira amostra do cpu_percent é ruidosa (janela curta desde a
            # chamada de inicialização); descarta para não inflar avg/max.
            if not first:
                samples.append((rss, cpu))
            first = False
            stop.wait(PROFILE_SAMPLE_INTERVAL)

    sampler = threading.Thread(target=_sampler, daemon=True)
    wall_start = time.time()
    cpu_start = sum(process.cpu_times()[:2])  # user + system
    sampler.start()
    try:
        yield
    finally:
        stop.set()
        sampler.join(timeout=5)
        wall = time.time() - wall_start
        cpu = sum(process.cpu_times()[:2]) - cpu_start
        rss_values = [s[0] for s in samples] or [process.memory_info().rss]
        cpu_values = [s[1] for s in samples if s[1] > 0]
        mb = 1024 * 1024

        print(f"===== PROFILE [{label}] =====")
        print(f"wall_time_s      = {wall:.2f}")
        print(f"cpu_time_s       = {cpu:.2f}")
        print(f"cpu_avg_percent  = {(sum(cpu_values) / len(cpu_values)) if cpu_values else 0:.1f}")
        print(f"cpu_max_percent  = {max(cpu_values) if cpu_values else 0:.1f}")
        print(f"rss_peak_mb      = {max(rss_values) / mb:.1f}")
        print(f"rss_avg_mb       = {(sum(rss_values) / len(rss_values)) / mb:.1f}")
        print(f"samples          = {len(samples)}")
        print("=============================")
