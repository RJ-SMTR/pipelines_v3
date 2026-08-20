# -*- coding: utf-8 -*-
"""
Flow de materialização dos dados do Rio Rotativo Digital

Executa o selector DBT 'riorotativo_auxiliar' (stagings, modelos de estado
atual, históricos diários e o histórico de vigência das entidades
credenciadoras) e o selector 'snapshot_riorotativo' (área de
estacionamento e perfil de funcionamento).

Backfill/reprocessamento: executar manualmente com datetime_start e
datetime_end — os modelos de estado atual saem do DAG automaticamente
quando a janela não alcança a data corrente (macro is_current_state_enabled)
e o ponteiro do Redis não regride.

DBT: 2026-07-08
"""

from datetime import time
from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__riorotativo_auxiliar import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__riorotativo_auxiliar(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.RIOROTATIVO_AUX_SELECTOR],
        snapshot_selector=constants.SNAPSHOT_RIOROTATIVO_AUX_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
        test_scheduled_time=time(5, 10, 0),
        test_webhook_key="alertas_bilhetagem",
    )
