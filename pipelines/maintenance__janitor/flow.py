# -*- coding: utf-8 -*-

from prefect import flow
from tasks import delete_stale_pending_runs


@flow(log_prints=True)
async def maintenance__janitor():
    await delete_stale_pending_runs(threshold_hours=1, batch_size=200)
