# -*- coding: utf-8 -*-
from typing import Optional

from pipelines.capture__jae_fiscalizacao_veiculo import constants
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.jae.tasks import create_jae_general_extractor
from pipelines.common.utils.prefect import flow


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__jae_fiscalizacao_veiculo(
    env: Optional[str] = None,
    timestamp: Optional[str] = None,
    recapture: bool = False,
    recapture_days: int = 2,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.FISCALIZACAO_VEICULO_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
