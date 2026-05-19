# -*- coding: utf-8 -*-
"""Tasks de profiling e download de queries em runtime"""

import os
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager

import psutil
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipelines.common.utils.fs import get_project_root_path
from pipelines.control__profiling_dbt.constants import (
    GITHUB_TOKEN_ENV,
    PROFILE_SAMPLE_INTERVAL,
)


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
        while not stop.is_set():
            try:
                samples.append((process.memory_info().rss, process.cpu_percent(None)))
            except psutil.Error:
                break
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


@task(cache_policy=NO_CACHE)
def fetch_queries(git_repo_url: str, git_ref: str) -> str:
    """
    Baixa apenas a pasta `queries` do repositório em runtime via git sparse-checkout.

    O conteúdo é colocado em `<project_root>/queries`, substituindo qualquer
    pasta existente, de modo que `run_dbt`/`get_project_root_path` funcionem
    sem alteração.

    Args:
        git_repo_url (str): URL HTTPS do repositório.
        git_ref (str): Branch, tag ou commit a ser baixado.

    Returns:
        str: Caminho absoluto da pasta queries baixada.
    """
    root = get_project_root_path()
    dest = root / "queries"

    token = os.getenv(GITHUB_TOKEN_ENV)
    clone_url = git_repo_url
    if token:
        clone_url = git_repo_url.replace("https://", f"https://x-access-token:{token}@")
        print("Usando token do GitHub para autenticação")
    else:
        print(f"{GITHUB_TOKEN_ENV} não definido, tentando clone sem autenticação")

    tmp_dir = root / "_queries_clone"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    with profile_resources("fetch_queries"):
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--filter=blob:none",
                "--sparse",
                "--branch",
                git_ref,
                clone_url,
                str(tmp_dir),
            ],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(tmp_dir), "sparse-checkout", "set", "queries"],
            check=True,
        )

        src = tmp_dir / "queries"
        if not src.is_dir():
            raise FileNotFoundError(f"Pasta 'queries' não encontrada no ref {git_ref}")

        if dest.exists():
            shutil.rmtree(dest)
        shutil.move(str(src), str(dest))
        shutil.rmtree(tmp_dir)

    file_count = sum(1 for _ in dest.rglob("*") if _.is_file())
    print(f"queries baixada em {dest} ({file_count} arquivos)")
    return str(dest)


def _dbt_runner() -> PrefectDbtRunner:
    project_dir = get_project_root_path() / "queries"
    target_path = project_dir / "target"
    os.environ["DBT_PROJECT_DIR"] = str(project_dir)
    os.environ["DBT_PROFILES_DIR"] = str(project_dir)
    os.environ["DBT_TARGET_PATH"] = str(target_path)
    return PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=project_dir,
            target_path=target_path,
        ),
        raise_on_failure=True,
    )


@task(cache_policy=NO_CACHE)
def run_dbt_deps():
    """Executa `dbt deps` na pasta queries baixada, com profiling."""
    with profile_resources("dbt_deps"):
        _dbt_runner().invoke(["deps"])


@task(cache_policy=NO_CACHE)
def run_dbt_select(dbt_select: str):
    """
    Executa `dbt run` (ou `dbt debug` se vazio) escrevendo no target `dev`.

    Args:
        dbt_select (str): Expressão `--select`. Se vazia, roda apenas `dbt debug`.
    """
    if dbt_select:
        invoke = ["run", "--select", dbt_select, "--target", "dev"]
    else:
        invoke = ["debug", "--target", "dev"]
    print(f"dbt {' '.join(invoke)}")
    with profile_resources(f"dbt_{invoke[0]}"):
        _dbt_runner().invoke(invoke)
