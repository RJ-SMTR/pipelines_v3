# -*- coding: utf-8 -*-
"""
Flow de materialização das views do datario.

Executa o selector DBT 'datario' para materializar as views do datario no BigQuery.

Schedule:
- Sem schedule (execução manual/ad-hoc)

DBT: 2025-11-06
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__datario import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__datario(
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.DATARIO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
    )
