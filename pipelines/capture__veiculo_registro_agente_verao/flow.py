# -*- coding: utf-8 -*-
"""
Flow de captura de registro de agentes de verão de veículos SPPO

Executa a captura de dados de registro de agentes de verão via API,
aplica tratamento padrão e carrega em BigQuery.

Common: 2026-03-19
"""

from prefect import flow

from pipelines.capture__veiculo_registro_agente_verao import constants
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run

sources = constants.SPPO_REGISTRO_AGENTE_VERAO_SOURCES


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__veiculo_registro_agente_verao(  # noqa: PLR0913
    env=None,
    source_table_ids=tuple([s.table_id for s in sources]),
    timestamp=None,
    recapture=True,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=sources,
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
