# -*- coding: utf-8 -*-
"""
Flow de captura de dados de GPS realocacao da Conecta
"""

from pipelines.capture__conecta_realocacao import constants
from pipelines.common.capture.gps.tasks import create_gps_extractor
from prefect import flow

from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__conecta_realocacao(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.CONECTA_REALOCACAO_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_gps_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
