# -*- coding: utf-8 -*-
"""
Flow de verificação de atualização (freshness) de fontes dbt

Executa testes de atualização a cada hora e notifica Discord se houver fontes desatualizadas.

Schedule: Horário (cron: "0 * * * *")

Common: 2026-03-10
"""

from prefect import flow, runtime

from pipelines.common.tasks import get_run_env, initialize_sentry, setup_environment
from pipelines.common.treatment.default_treatment.utils import run_dbt
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.control__source_freshness import constants
from pipelines.control__source_freshness.tasks import (
    parse_source_freshness_output,
    source_freshness_notify_discord,
)


@flow(
    log_prints=True,
    on_failure=[handler_notify_failure(webhook=constants.DISCORD_WEBHOOK_KEY)],
)
def control__source_freshness(env=None, flags=None):
    """
    Verifica a atualização de fontes dbt e notifica Discord se houver problemas.

    Args:
        env (Optional[str]): Ambiente de execução (prod ou dev). Se não informado, será detectado
            automaticamente baseado no deployment ou será 'dev' se executando localmente.
        flags (Optional[list[str]]): Flags adicionais para o dbt.

    Returns:
        None
    """
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_environment(env=env)
    initialize_sentry(env)

    dbt_output = run_dbt(dbt_command="source freshness", flags=flags)

    has_issues, failed_sources = parse_source_freshness_output(dbt_output=dbt_output)

    if has_issues and failed_sources:
        source_freshness_notify_discord(failed_sources=failed_sources)
