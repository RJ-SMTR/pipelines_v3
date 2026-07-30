# -*- coding: utf-8 -*-
"""
Flow de materialização da infraestrutura

Executa o selector DBT 'infraestrutura' para materializar dados no BigQuery.

Schedule:
- Diariamente às 9h00 (horário de São Paulo)

DBT: 2026-04-27
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__infraestrutura import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__infraestrutura(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
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
