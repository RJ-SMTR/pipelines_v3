# -*- coding: utf-8 -*-
"""
Flow de materialização de dados do GPS 15 minutos SPPO

Executa o selector DBT 'gps_15_minutos' para materializar dados no BigQuery.

Schedule:
- A cada 15 minutos (horário de São Paulo)
- Envia notificações em caso de falha

DBT: 2026-04-17
"""

from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.treatment__gps_15_minutos_sppo import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="dataplex")],
    on_crashed=[handler_notify_failure(webhook="dataplex")],
)
def treatment__gps_15_minutos_sppo(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = {
        "modo_gps": "onibus",
        "fonte_gps": "sppo",
        "15_minutos": True,
    },
    force_test_run: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.GPS_15_MINUTOS_SPPO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
    )
