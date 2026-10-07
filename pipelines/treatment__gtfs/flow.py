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


@flow(log_prints=True, flow_run_name=rename_flow_run)
async def treatment__gtfs(  # noqa: PLR0913
    data_versao: str,
    env: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
):
    data_versao = data_versao[:10]
    tasks = create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.GTFS_SELECTOR],
        datetime_start=data_versao,
        datetime_end=data_versao,
        flags=flags,
        additional_vars={**(additional_vars or {}), "data_versao_gtfs": data_versao},
        force_test_run=force_test_run,
        skip_source_check=skip_source_check,
        test_webhook_key=constants.GTFS_DISCORD_WEBHOOK,
        save_redis=False,
    )

    datetime_start, datetime_end, planejamento_vars = get_planejamento_materialization_window(
        data_versao_gtfs=data_versao,
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
        wait_for_completion=False,
    )
