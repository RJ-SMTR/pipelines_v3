# -*- coding: utf-8 -*-
from typing import Optional

from pipelines.capture__smtr_controle_financeiro import constants
from pipelines.capture__smtr_controle_financeiro.tasks import create_controle_financeiro_extractor
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.utils.prefect import flow


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__smtr_controle_financeiro(
    env: Optional[str] = None,
    timestamp: Optional[str] = None,
    source_table_ids=tuple([t.table_id for t in constants.CONTROLE_FINANCEIRO_SOURCES]),
):
    create_capture_flows_default_tasks(
        env=env,
        sources=constants.CONTROLE_FINANCEIRO_SOURCES,
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        create_extractor_task=create_controle_financeiro_extractor,
        recapture=False,
        recapture_days=2,
        recapture_timestamps=None,
    )
