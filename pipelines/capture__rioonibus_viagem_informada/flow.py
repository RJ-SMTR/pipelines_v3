# -*- coding: utf-8 -*-
"""
Flow de captura de viagens informadas da Rio Ônibus

Executa a captura de dados de informação de viagens da API da Rio Ônibus.

Common: 2026-03-06
"""

from prefect import flow

from pipelines.capture__rioonibus_viagem_informada import constants
from pipelines.capture__rioonibus_viagem_informada.tasks import create_viagem_informada_extractor
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run

sources = [constants.VIAGEM_INFORMADA_SOURCE]


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__rioonibus_viagem_informada(
    env=None,
    timestamp=None,
    recapture=True,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=sources,
        timestamp=timestamp,
        create_extractor_task=create_viagem_informada_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
