# -*- coding: utf-8 -*-
import asyncio
from os import getenv

from tasks import delete_old_flow_runs, vacuum_index_bloat, vacuum_tables

from pipelines.common.utils.prefect import flow


@flow(log_prints=True)
async def maintenance__db_retention(
    days_to_keep: int = 25, batch_size: int = 100, bloat_threshold: float = 30.0
):
    db_url = getenv("PREFECT_DB_URL")
    await delete_old_flow_runs(days_to_keep=days_to_keep, batch_size=batch_size)
    await vacuum_tables(db_url=db_url, bloat_threshold=bloat_threshold)
    await vacuum_index_bloat(db_url=db_url, bloat_threshold=bloat_threshold)


if __name__ == "__main__":
    asyncio.run(maintenance__db_retention())
