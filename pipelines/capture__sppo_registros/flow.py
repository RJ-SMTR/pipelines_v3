# -*- coding: utf-8 -*-
"""
Flow de captura de dados de GPS registros do SPPO

Common: 2026-04-16
"""

from typing import Optional

from prefect import flow

from pipelines.capture__sppo_registros import constants
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.gps.tasks import create_gps_extractor


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__sppo_registros(
    env: Optional[str] = None,
    timestamp: Optional[str] = None,
    recapture: bool = False,
    recapture_days: int = 2,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.SPPO_REGISTROS_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_gps_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
