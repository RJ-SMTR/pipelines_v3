# -*- coding: utf-8 -*-
"""
Flow de materialização de dados do GPS zirix

Executa o selector DBT 'gps' para materializar dados no BigQuery.

Schedule:
- A cada hora, no minuto 6 (horário de São Paulo)
- Envia notificações em caso de falha

DBT: 2026-04-16
"""

from datetime import time

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.treatment__gps_zirix import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="dataplex")],
    on_crashed=[handler_notify_failure(webhook="dataplex")],
)
def treatment__gps_zirix(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars={"modo_gps": "onibus", "fonte_gps": "zirix", "15_minutos": False},  # noqa: B006
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.GPS_ZIRIX_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=time(2, 6, 0),
        force_test_run=force_test_run,
    )
