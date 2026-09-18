# -*- coding: utf-8 -*-
"""Tasks: download de queries em runtime e execução de dbt com profiling."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import yaml
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipelines.common.utils.fs import get_project_root_path
from pipelines.common.utils.profiling import profile_resources
from pipelines.control__profiling_dbt_runtime.constants import GITHUB_TOKEN_ENV


@task(cache_policy=NO_CACHE)
def fetch_queries(git_repo_url: str, git_ref: str) -> str:
    """
    Baixa apenas a pasta `queries` do repositório em runtime via git sparse-checkout.

    Coloca o conteúdo em `<project_root>/queries`, substituindo qualquer pasta
    existente, para que helpers comuns (`get_project_root_path`) funcionem
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
            project_dir=Path(project_dir),
            profiles_dir=Path(project_dir),
            target_path=Path(target_path),
        ),
        raise_on_failure=True,
    )


@task(cache_policy=NO_CACHE)
def run_dbt_deps():
    """Executa `dbt deps` na pasta queries baixada, com profiling."""
    with profile_resources("dbt_deps"):
        _dbt_runner().invoke(["deps"])


@task(cache_policy=NO_CACHE)
def run_dbt_select(
    dbt_select: str,
    dbt_user: str = "botelho",
    dbt_selector: str = "",
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
):
    """
    Executa `dbt run` escrevendo no target `dev`.

    Usa `--selector` quando `dbt_selector` é passado, `--select` quando `dbt_select`
    é passado. Se ambos vazios, roda apenas `dbt debug`.

    Args:
        dbt_select (str): Expressão `--select`.
        dbt_user (str): Valor de `DBT_USER` usado pelo macro `generate_schema_name`
            para prefixar o schema em target `dev`.
        dbt_selector (str): Nome do `--selector` definido em `selectors.yml`.
        datetime_start (Optional[str]): Datetime inicial (ISO) passado como `--vars`.
        datetime_end (Optional[str]): Datetime final (ISO) passado como `--vars`.
        flags (Optional[list[str]]): Flags adicionais anexadas ao comando dbt.
        additional_vars (Optional[dict]): Variáveis extras passadas via `--vars`.
    """
    os.environ["DBT_USER"] = dbt_user
    print(f"DBT_USER={dbt_user}")
    if dbt_selector:
        invoke = ["run", "--selector", dbt_selector, "--target", "dev"]
    elif dbt_select:
        invoke = ["run", "--select", dbt_select, "--target", "dev"]
    else:
        invoke = ["debug", "--target", "dev"]

    dbt_vars: dict = {}
    if datetime_start:
        dbt_vars["date_range_start"] = datetime_start
    if datetime_end:
        dbt_vars["date_range_end"] = datetime_end
    if additional_vars:
        dbt_vars.update(additional_vars)

    if dbt_vars and invoke[0] != "debug":
        vars_yaml = yaml.safe_dump(dbt_vars, default_flow_style=True)
        invoke = [*invoke, "--vars", vars_yaml]

    if flags:
        invoke = [*invoke, *flags]

    print(f"dbt {' '.join(invoke)}")
    with profile_resources(f"dbt_{invoke[0]}"):
        _dbt_runner().invoke(invoke)
