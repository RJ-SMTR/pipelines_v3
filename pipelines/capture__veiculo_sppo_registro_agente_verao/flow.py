# -*- coding: utf-8 -*-
"""
Flow de captura de registro de agentes de verão do SPPO

Executa a captura de dados de registro de agentes que atuam durante
o período de verão.

Common: 2026-03-31
"""

from prefect import flow

from pipelines.capture__veiculo_sppo_registro_agente_verao import constants
from pipelines.capture__veiculo_sppo_registro_agente_verao.tasks import (
    create_sppo_agentes_verao_extractor,
)
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__veiculo_sppo_registro_agente_verao(
    env=None,
    timestamp=None,
    recapture=True,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=constants.SPPO_REGISTRO_AGENTE_VERAO_SOURCES,
        timestamp=timestamp,
        create_extractor_task=create_sppo_agentes_verao_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
