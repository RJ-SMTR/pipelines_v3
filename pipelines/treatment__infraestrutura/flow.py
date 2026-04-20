# -*- coding: utf-8 -*-
"""
Flow de materialização da infraestrutura

Executa o selector DBT 'infraestrutura' para materializar dados no BigQuery.

Schedule:
- Diariamente às 9h00 (horário de São Paulo)

DBT: 2026-04-20
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__infraestrutura import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__infraestrutura(
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.INFRAESTRUTURA_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_INFRAESTRUTURA_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
    )
