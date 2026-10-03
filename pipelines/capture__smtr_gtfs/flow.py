# -*- coding: utf-8 -*-
"""Flow genérico de captura dos arquivos do GTFS."""

from typing import Optional

from pipelines.capture__smtr_gtfs import constants
from pipelines.capture__smtr_gtfs.tasks import (
    create_gtfs_extractor,
    should_capture_gtfs,
    update_last_materialized_os,
)
from pipelines.common.capture.default_capture.flow import (
    create_capture_flows_default_tasks,
)
from pipelines.common.tasks import run_subflow
from pipelines.common.utils.prefect import flow, rename_flow_run
from pipelines.treatment__gtfs.flow import treatment__gtfs


@flow(log_prints=True, flow_run_name=rename_flow_run, timeout_seconds=7200)
async def capture__smtr_gtfs(
    env: Optional[str] = None,
    upload_from_gcs: bool = False,  # noqa: ARG001
    regular_sheet_index: Optional[int] = None,  # noqa: ARG001
    data_versao_gtfs: Optional[str] = None,  # noqa: ARG001
    flags: Optional[list[str]] = None,
):
    tasks = create_capture_flows_default_tasks(
        env=env,
        sources=constants.GTFS_SOURCES,
        timestamp=None,
        create_extractor_task=create_gtfs_extractor,
        recapture=False,
        recapture_days=0,
        recapture_timestamps=[],
        should_capture_task=should_capture_gtfs,
    )

    capture_info = tasks["should_capture_result"].payload or {}
    if not tasks["should_capture"] and not capture_info.get("pending_materialization"):
        return

    await run_subflow(
        env=tasks["env"],
        flow=treatment__gtfs,
        parameters=[
            {
                "env": tasks["env"],
                "flags": flags,
                "additional_vars": {
                    "data_versao_gtfs": capture_info["data_versao_gtfs"],
                },
            }
        ],
        wait_for_completion=True,
    )
    update_last_materialized_os(
        dataset_id=constants.GTFS_DATASET_ID,
        data_index=capture_info["data_index"],
        mode=tasks["env"],
        advance_marker=capture_info["advance_last_materialized"],
    )
