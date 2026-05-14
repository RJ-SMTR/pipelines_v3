# -*- coding: utf-8 -*-
"""
Flow de verificação de freshness de modelos via testes do Elementary

Executa os testes de freshness do Elementary (tag `elementary`) e notifica Discord
caso haja modelos com anomalia de atualização.

Schedule: Horário (cron: "0 * * * *")
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import runtime

from pipelines.common import constants as smtr_constants
from pipelines.common.tasks import get_run_env, initialize_sentry, setup_environment
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
def control__model_freshness(env=None, test_select: str = "tag:freshness_hourly"):
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
    setup_environment(env=env)
    initialize_sentry(env)

    dbt_test = DBTTest(test_select=test_select)
    now = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE))
    dbt_logs, _ = run_dbt_tests(dbt_test=dbt_test, datetime_start=now, datetime_end=now)

    has_issues, failed_results = parse_model_freshness_output(dbt_output=dbt_logs)

    if has_issues and failed_results:
        model_freshness_notify_discord(failed_results=failed_results, test_select=test_select)
