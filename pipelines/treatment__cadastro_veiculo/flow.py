# -*- coding: utf-8 -*-
"""
Flow de materialização de cadastro de veículos

Executa os selectors DBT 'cadastro_veiculo' e 'snapshot_cadastro_veiculo' para
materializar dados de licenciamento no BigQuery.

DBT 2026-05-05
"""

from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__cadastro_veiculo import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__cadastro_veiculo(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict[str, str]] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.CADASTRO_VEICULO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
        snapshot_selector=constants.CADASTRO_VEICULO_SNAPSHOT_SELECTOR,
    )
