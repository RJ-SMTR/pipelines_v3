# -*- coding: utf-8 -*-
"""
Flows de teste para avaliação de consumo de recursos.

Common: 2026-05-19
"""

from pipelines.common.tasks import setup_environment
from pipelines.common.utils.prefect import flow
from pipelines.control__profiling_dbt.constants import DEFAULT_GIT_REF, GIT_REPO_URL
from pipelines.control__profiling_dbt.tasks import (
    fetch_queries,
    profile_resources,
    run_dbt_deps,
    run_dbt_select,
)


@flow(log_prints=True)
def control__profiling_baseline():
    """Flow vazio instrumentado — baseline de consumo de recursos."""
    with profile_resources("baseline_flow"):
        print("Flow baseline: nenhuma operação executada.")


@flow(log_prints=True)
def control__profiling_dbt_runtime(
    git_ref: str = DEFAULT_GIT_REF,
    git_repo_url: str = GIT_REPO_URL,
    dbt_select: str = "",
):
    """
    Baixa queries em runtime, roda dbt deps e dbt, sempre escrevendo em `dev`.

    Args:
        git_ref: Branch/tag/commit do qual baixar a pasta queries.
        git_repo_url: URL HTTPS do repositório.
        dbt_select: Expressão `--select`. Vazio = apenas `dbt debug`.
    """
    with profile_resources("dbt_runtime_flow_total"):
        setup_environment(env="dev")
        queries_path = fetch_queries(git_repo_url=git_repo_url, git_ref=git_ref)
        deps = run_dbt_deps(wait_for=[queries_path])
        run_dbt_select(dbt_select=dbt_select, wait_for=[deps])
