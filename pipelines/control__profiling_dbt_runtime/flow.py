# -*- coding: utf-8 -*-
"""
Flow de teste: baixa a pasta `queries` do repositório em runtime, roda
`dbt deps` e dbt escrevendo no target `dev`, tudo instrumentado com profiling.

Valida a remoção da pasta `queries` da imagem base — em vez de embutir no
container, o flow obtém o projeto dbt em runtime.

Common: 2026-05-19
"""

from typing import Optional

from pipelines.common.tasks import setup_environment
from pipelines.common.utils.prefect import flow
from pipelines.common.utils.profiling import profile_resources
from pipelines.control__profiling_dbt_runtime.constants import DEFAULT_GIT_REF, GIT_REPO_URL
from pipelines.control__profiling_dbt_runtime.tasks import (
    fetch_queries,
    run_dbt_deps,
    run_dbt_select,
)


@flow(log_prints=True)
def control__profiling_dbt_runtime(
    git_ref: str = DEFAULT_GIT_REF,
    git_repo_url: str = GIT_REPO_URL,
    dbt_select: str = "",
    dbt_selector: str = "",
    dbt_user: str = "botelho",
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict[str, str]] = None,
):
    """
    Baixa queries em runtime, roda dbt deps e dbt, sempre escrevendo em `dev`.

    Args:
        git_ref: Branch/tag/commit do qual baixar a pasta queries.
        git_repo_url: URL HTTPS do repositório.
        dbt_select: Expressão `--select`. Vazio = apenas `dbt debug` (se selector também vazio).
        dbt_selector: Nome do `--selector` definido em `selectors.yml`. Tem prioridade sobre `dbt_select`.
        dbt_user: Valor de `DBT_USER` (prefixo de schema em target `dev`).
        datetime_start: Datetime inicial (ISO) passado como `--vars date_range_start`.
        datetime_end: Datetime final (ISO) passado como `--vars date_range_end`.
        flags: Flags adicionais do dbt (ex: ["--full-refresh"]).
        additional_vars: Variáveis extras passadas via `--vars`.
    """
    with profile_resources("dbt_runtime_flow_total"):
        setup_environment(env="dev")
        queries_path = fetch_queries(git_repo_url=git_repo_url, git_ref=git_ref)
        deps = run_dbt_deps(wait_for=[queries_path])
        run_dbt_select(
            dbt_select=dbt_select,
            dbt_selector=dbt_selector,
            dbt_user=dbt_user,
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            flags=flags,
            additional_vars=additional_vars,
            wait_for=[deps],
        )
