# -*- coding: utf-8 -*-
"""
Flow de materialização de dados de veículo dia

Executa o selector DBT 'veiculo_dia' para materializar dados no BigQuery.

DBT: 2026-04-22
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__veiculo_dia import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__veiculo_dia(
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
    skip_source_check=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.VEICULO_DIA_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
