# -*- coding: utf-8 -*-
import asyncio
from os import getenv, environ

from tasks import delete_old_flow_runs, vacuum_index_bloat, vacuum_tables

from pipelines.common.utils.prefect import flow


@flow(log_prints=True)
async def maintenance__db_retention():
    db_url = getenv("PREFECT_DB_URL")
    await delete_old_flow_runs(days_to_keep=15, batch_size=200)
    await vacuum_tables(db_url=db_url, bloat_threshold=30.0)
    await vacuum_index_bloat(db_url=db_url, bloat_threshold=30.0)


if __name__ == "__main__":
    asyncio.run(maintenance__db_retention())
