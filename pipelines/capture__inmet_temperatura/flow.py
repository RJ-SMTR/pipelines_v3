# -*- coding: utf-8 -*-
"""
Flow de captura de dados de temperatura do INMET

Executa a captura de dados de temperatura das estações meteorológicas do INMET
para as análises de monitoramento de temperatura dos veículos da SMTR.

Common: 2026-05-13
"""

from pipelines.capture__inmet_temperatura import constants
from pipelines.capture__inmet_temperatura.tasks import create_temperatura_extractor
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.utils.prefect import flow

sources = constants.INMET_TEMPERATURA_SOURCES


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__inmet_temperatura(  # noqa: PLR0913
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
        create_extractor_task=create_temperatura_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
