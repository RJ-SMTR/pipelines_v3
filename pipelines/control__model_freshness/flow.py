# -*- coding: utf-8 -*-
"""
Flow de verificação de freshness de modelos via testes do Elementary

Executa os testes de freshness do Elementary (tag `elementary`) e notifica Discord
caso haja modelos com anomalia de atualização.

Schedule: Horário (cron: "0 * * * *")
"""

from datetime import timedelta
from typing import Optional

from prefect import runtime

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    install_dbt_packages,
    setup_dbt_queries,
)
from pipelines.common.treatment.default_treatment.utils import DBTTest, run_dbt_tests
from pipelines.common.utils.prefect import flow, handler_notify_failure
from pipelines.control__model_freshness.tasks import (
    model_freshness_notify_discord,
    parse_model_freshness_output,
)


@flow(
    log_prints=True,
    flow_run_name="control__model_freshness - {test_select}",
    on_failure=[handler_notify_failure(webhook="dataplex")],
)
def control__model_freshness(env: Optional[str] = None, test_select: str = "tag:freshness_hourly"):
    """
    Roda testes de freshness em modelos dbt (selecionados por tag) e notifica
    Discord caso haja falhas.

    Args:
        env (Optional[str]): Ambiente de execução (prod ou dev). Se não informado, será detectado
            automaticamente baseado no deployment ou será 'dev' se executando localmente.
        test_select (str): Selector dbt usado para filtrar os testes executados.
            Ex.: "tag:freshness_daily", "tag:freshness_hourly".
    """
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)
    sentry = initialize_sentry(env)
    queries = setup_dbt_queries(wait_for=[setup_env])
    install_dbt_packages(wait_for=[queries])

    dbt_test = DBTTest(test_select=test_select)
    timestamp = get_scheduled_timestamp(wait_for=[sentry])

    dbt_logs, _ = run_dbt_tests(
        dbt_test=dbt_test,
        datetime_start=timestamp - timedelta(hours=2),
        datetime_end=timestamp,
        env=env,
    )

    has_issues, failed_results = parse_model_freshness_output(dbt_output=dbt_logs)

    if has_issues and failed_results:
        model_freshness_notify_discord(failed_results=failed_results, test_select=test_select)
