# -*- coding: utf-8 -*-
"""
Flow de materialização de dados de cliente com CPF

Executa o selector DBT 'cliente_cpf' para materializar dados no BigQuery.

DBT: 2026-03-06
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__cliente_cpf import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__cliente_cpf(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.CLIENTE_CPF_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
    )
