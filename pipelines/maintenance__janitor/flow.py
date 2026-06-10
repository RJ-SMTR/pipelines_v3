# -*- coding: utf-8 -*-

from tasks import delete_stale_pending_runs

from pipelines.common.utils.prefect import flow
import pipelines.common as common


@flow(log_prints=True)
async def maintenance__janitor():
    await delete_stale_pending_runs(threshold_hours=1, batch_size=200)
