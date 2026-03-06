# -*- coding: utf-8 -*-
"""
Flow de materialização de dados do GPS do validador

Executa o selector DBT 'gps_validador' para materializar dados no BigQuery.

Schedule:
- Diariamente às 1h15 (horário de São Paulo)
- Envia notificações em caso de falha

DBT: 2026-03-06
"""

from datetime import time

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.treatment__gps_validador import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def treatment__gps_validador(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.GPS_VALIDADOR_SELECTOR],
        test_webhook_key="alertas_bilhetagem",
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=time(1, 15, 0),
        force_test_run=force_test_run,
    )
