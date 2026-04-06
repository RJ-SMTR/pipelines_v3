# -*- coding: utf-8 -*-
"""
Flow de materialização das integrações

Executa o selector DBT 'integracao' para materializar dados no BigQuery.

DBT: 2026-03-31
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.treatment__integracao import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def treatment__integracao(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    fallback_run=False,
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.INTEGRACAO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        fallback_run=fallback_run,
        force_test_run=force_test_run,
    )
