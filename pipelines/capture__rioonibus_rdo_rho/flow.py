# -*- coding: utf-8 -*-
"""Flow para captura dos dados do RDO"""

from typing import Optional

from prefect import flow

from pipelines.capture__rioonibus_rdo_rho import constants
from pipelines.capture__rioonibus_rdo_rho.tasks import create_rdo_general_extractor
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.utils.prefect import handler_notify_failure


@flow(
    log_prints=True,
    flow_run_name=rename_capture_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def capture__rioonibus_rdo_rho(
    env: Optional[str] = None,
    timestamp: Optional[str] = None,
    recapture: bool = True,
    recapture_days: int = 10,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=constants.RDO_SOURCES,
        timestamp=timestamp,
        create_extractor_task=create_rdo_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
