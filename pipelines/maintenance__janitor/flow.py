# -*- coding: utf-8 -*-
import random

import pandas as pd
from prefect import flow, task

from tasks import delete_old_flow_runs, delete_stale_pending_runs


@flow(name="database-retention")
async def maintenance__retention_flow():
    """Run database retention tasks."""
    await delete_old_flow_runs(
        days_to_keep=15,
        batch_size=200
    )

@flow(log_prints=True)
def maintenance__janitor() -> list[str]:
    delete_stale_pending_runs(
        threshold_hours=1,
        batch_size=200
    )
    