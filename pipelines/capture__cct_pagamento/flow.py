# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from pipelines.capture__cct_pagamento import constants
from pipelines.capture__cct_pagamento.tasks import create_cct_general_extractor
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run

sources = constants.PAGAMENTO_SOURCES


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__cct_pagamento(  # noqa: PLR0913
    env: Optional[str] = None,
    source_table_ids: list[str] = tuple([s.table_id for s in sources]),
    timestamp: Optional[str] = None,
    recapture: bool = True,
    recapture_days: int = 2,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=sources,
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        create_extractor_task=create_cct_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
