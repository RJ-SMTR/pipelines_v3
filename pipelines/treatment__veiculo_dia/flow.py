# -*- coding: utf-8 -*-
"""
Flow de materialização de dados do veiculo_dia

Executa o selector DBT 'veiculo_dia' para materializar dados no BigQuery.

Schedule:
- Diariamente às 6h15 (horário de São Paulo)
- Depende de dados do capture__veiculo_fiscalizacao_lacre e capture__veiculo_sppo_agente_verao

DBT: 2026-05-12

"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__veiculo_dia import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__veiculo_dia(  # noqa: PLR0913
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
        selectors=[constants.VEICULO_DIA_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_VEICULO_DIA_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
    )
