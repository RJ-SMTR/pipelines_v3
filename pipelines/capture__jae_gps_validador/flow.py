# -*- coding: utf-8 -*-
"""
Flow para captura dos dados do GPS do validador da Jaé

Common 2026-02-09
"""

from prefect import flow

from pipelines.capture__jae_gps_validador import constants
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.jae.tasks import create_jae_general_extractor


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__jae_gps_validador(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.GPS_VALIDADOR_SOURCE],
        source_table_ids=[constants.GPS_VALIDADOR_SOURCE.table_id],
        timestamp=timestamp,
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
