# -*- coding: utf-8 -*-
"""Flow genérico de materialização do GTFS e do planejamento diário relacionado."""

from typing import Optional

from pipelines.common.tasks import run_subflow
from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.utils.prefect import flow, rename_flow_run
from pipelines.treatment__gtfs import constants
from pipelines.treatment__gtfs.tasks import get_planejamento_materialization_window
from pipelines.treatment__planejamento_diario.flow import treatment__planejamento_diario


@flow(log_prints=True, flow_run_name=rename_flow_run, timeout_seconds=7200)
async def treatment__gtfs(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
):
    additional_vars = additional_vars or {}
    data_versao_gtfs = additional_vars.get("data_versao_gtfs")
    if data_versao_gtfs is None:
        raise ValueError("additional_vars.data_versao_gtfs é obrigatório.")

    tasks = create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.create_gtfs_selector(data_versao_gtfs)],
        datetime_start=datetime_start or data_versao_gtfs,
        datetime_end=datetime_end or data_versao_gtfs,
        flags=flags,
        additional_vars=additional_vars,
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
        test_webhook_key=constants.GTFS_DISCORD_WEBHOOK,
        save_redis=False,
    )

    datetime_start, datetime_end, planejamento_vars = get_planejamento_materialization_window(
        data_versao_gtfs=data_versao_gtfs,
        env=tasks["env"],
        flags=flags,
        wait_for=[tasks["save_redis"]],
    )
    await run_subflow(
        env=tasks["env"],
        flow=treatment__planejamento_diario,
        parameters=[
            {
                "env": tasks["env"],
                "datetime_start": datetime_start,
                "datetime_end": datetime_end,
                "flags": flags,
                "additional_vars": planejamento_vars,
            }
        ],
        wait_for_completion=True,
    )
