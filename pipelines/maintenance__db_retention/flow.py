# -*- coding: utf-8 -*-
from tasks import delete_old_flow_runs

from pipelines.common.utils.prefect import flow


@flow(log_prints=True)
async def maintenance__db_retention():
    await delete_old_flow_runs(days_to_keep=15, batch_size=200)
