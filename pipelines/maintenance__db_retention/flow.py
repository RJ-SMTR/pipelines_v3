# -*- coding: utf-8 -*-
from os import getenv
from tasks import delete_old_flow_runs, optimize_database_if_needed

from pipelines.common.utils.prefect import flow


@flow(log_prints=True)
async def maintenance__db_retention():
    db_url = getenv('PREFECT_DB_URL')
    await delete_old_flow_runs(days_to_keep=15, batch_size=200)
    await optimize_database_if_needed(db_url=db_url, bloat_threshold=30.0)
