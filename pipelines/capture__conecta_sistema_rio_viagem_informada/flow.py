# -*- coding: utf-8 -*-
"""
Flow de captura de viagens informadas da Conecta para o Sistema RIO.
"""

from typing import Optional

from pipelines.capture__conecta_sistema_rio_viagem_informada import constants
from pipelines.capture__conecta_sistema_rio_viagem_informada.tasks import (
    create_viagem_informada_extractor,
)
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.utils.prefect import flow


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__conecta_sistema_rio_viagem_informada(
    env: Optional[str] = None,
    timestamp: Optional[str] = None,
    recapture: bool = False,
    recapture_days: int = 5,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.VIAGEM_INFORMADA_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_viagem_informada_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
