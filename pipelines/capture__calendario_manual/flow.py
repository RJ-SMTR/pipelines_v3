# -*- coding: utf-8 -*-
"""
Flow de captura do calendário manual (datas atípicas)

Lê a aba "Dias Atípicos" da planilha de planejamento e, quando há alteração, ingere os dados em
`source_smtr.calendario_manual` e dispara a materialização do `treatment__planejamento_diario`.

Common: 2026-06-19
"""

from functools import partial
from typing import Optional

from pipelines.capture__calendario_manual import constants
from pipelines.capture__calendario_manual.tasks import (
    create_calendario_manual_extractor,
    detect_calendario_change,
    get_calendario_materialization_window,
    update_calendario_hashes_by_date,
)
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.tasks import trigger_materialization
from pipelines.common.utils.prefect import flow
from pipelines.treatment__planejamento_diario.flow import treatment__planejamento_diario


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
async def capture__calendario_manual(  # noqa: PLR0913
    env: Optional[str] = None,
    source_table_ids: Optional[tuple[str]] = None,
    timestamp: Optional[str] = None,
    recapture: bool = False,
    recapture_days: int = 0,
    recapture_timestamps: Optional[list[str]] = None,
):
    tasks = create_capture_flows_default_tasks(
        env=env,
        sources=[constants.CALENDARIO_MANUAL_SOURCE],
        source_table_ids=source_table_ids,
        timestamp=timestamp,
        create_extractor_task=create_calendario_manual_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
        should_capture_task=detect_calendario_change,
    )

    if tasks["should_capture"]:
        payload = tasks["should_capture_result"].payload
        await trigger_materialization(
            env=tasks["env"],
            flow=treatment__planejamento_diario,
            window_fn=partial(
                get_calendario_materialization_window,
                changed_dates=payload["changed_dates"],
            ),
        )
        update_calendario_hashes_by_date(
            env=tasks["env"],
            hashes_by_date=payload["hashes_by_date"],
        )
