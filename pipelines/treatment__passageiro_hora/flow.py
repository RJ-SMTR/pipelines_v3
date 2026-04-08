# -*- coding: utf-8 -*-
"""
Flow de materialização de passageiros por hora

Executa o selector DBT 'passageiro_hora' para materializar dados no BigQuery.

Schedule:
- Diariamente às 0h35 (horário de São Paulo)

DBT: 2026-03-06
"""

from datetime import time

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__passageiro_hora import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__passageiro_hora(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.PASSAGEIRO_HORA_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=time(0, 35, 0),
        force_test_run=force_test_run,
        test_webhook_key="alertas_bilhetagem",
    )
