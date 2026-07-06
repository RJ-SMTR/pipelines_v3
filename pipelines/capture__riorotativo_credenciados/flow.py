# -*- coding: utf-8 -*-
"""
Flow de captura de dados de credenciados do Rio Rotativo

Captura abas de Google Sheets com dados de credenciados do Rio Rotativo.
"""

from typing import Optional

from pipelines.capture__riorotativo_credenciados import constants
from pipelines.capture__riorotativo_credenciados.tasks import (
    create_riorotativo_credenciados_extractor,
)
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.utils.prefect import flow


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__riorotativo_credenciados(  # noqa: PLR0913
    env: Optional[str] = None,
    source_table_ids: Optional[tuple[str]] = None,
    timestamp: Optional[str] = None,
    recapture: bool = True,
    recapture_days: int = 7,
    recapture_timestamps: Optional[list[str]] = None,
):
    create_capture_flows_default_tasks(
        env=env,
        sources=constants.RIOROTATIVO_CREDENCIADOS_SOURCES,
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        create_extractor_task=create_riorotativo_credenciados_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
        extra_parameters=constants.RIOROTATIVO_CREDENCIADOS_EXTRA_PARAMETERS,
    )
