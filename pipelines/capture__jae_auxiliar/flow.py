# -*- coding: utf-8 -*-
# common: 2025-01-07a
from prefect import flow

from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.capture.jae import constants
from pipelines.common.capture.jae.tasks import create_jae_general_extractor

sources = constants.JAE_AUXILIAR_SOURCES


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__jae_auxiliar(  # noqa: PLR0913
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
        create_extractor_task=create_jae_general_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )
