# -*- coding: utf-8 -*-
"""
Flow de materialização do planejamento diário

Executa o selector DBT 'planejamento_diario' para materializar dados no BigQuery.

Schedule:
- Diariamente às 1h00 (horário de São Paulo)
- Depende de dados capturados pela Rio Ônibus

DBT: 2026-05-22
"""

from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__planejamento_diario import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__planejamento_diario(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.PLANEJAMENTO_DIARIO_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_PLANEJAMENTO_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
    )
