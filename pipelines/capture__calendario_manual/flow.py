# -*- coding: utf-8 -*-
"""
Flow de captura do calendário manual (datas atípicas)

Lê a aba "Dias Atípicos" da planilha de planejamento e, quando há alteração, ingere os dados em
`source_smtr.calendario_manual` e dispara a materialização do `treatment__planejamento_diario`.

Common: 2026-06-19
"""

from typing import Optional

from pipelines.capture__calendario_manual import constants
from pipelines.capture__calendario_manual.tasks import (
    create_calendario_manual_extractor,
    detect_calendario_change,
    get_calendario_materialization_window,
    update_calendario_last_row_state,
)
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run
from pipelines.common.tasks import run_subflow
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
        window = get_calendario_materialization_window(
            changed_dates=payload["changed_dates"],
        )
        if window is not None:
            datetime_start, datetime_end = window
            await run_subflow(
                env=tasks["env"],
                flow=treatment__planejamento_diario,
                parameters=[
                    {
                        "env": tasks["env"],
                        "datetime_start": datetime_start,
                        "datetime_end": datetime_end,
                    }
                ],
                wait_for_completion=False,
            )
        update_calendario_last_row_state(
            env=tasks["env"],
            last_row_state=payload["last_row_state"],
        )
