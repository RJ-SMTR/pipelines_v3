# -*- coding: utf-8 -*-
"""
Flow de captura da transação riocard da Jaé

Executa a captura de dados de transação riocard do sistema Jaé.

Common: 2026-03-19
"""

from prefect import flow

from pipelines.capture__jae_transacao_riocard import constants
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.jae.tasks import create_jae_general_extractor


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__jae_transacao_riocard(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=[constants.TRANSACAO_RIOCARD_SOURCE],
        timestamp=timestamp,
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
