# -*- coding: utf-8 -*-
"""
Flow de materialização de monitoramento de veículo

Executa o selector DBT 'monitoramento_veiculo' para materializar dados de veículos
fiscalizados no BigQuery, incluindo lacres e autuações disciplinares.

Schedule:
- Diariamente às 5h45 (horário de São Paulo)
- Depende de dados de fiscalização de veículos

DBT: 2026-05-01
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__monitoramento_veiculo import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__monitoramento_veiculo(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.MONITORAMENTO_VEICULO_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_VEICULO_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
