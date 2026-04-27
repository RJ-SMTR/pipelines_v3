# -*- coding: utf-8 -*-

from prefect import flow
from tasks import delete_stale_pending_runs


@flow(log_prints=True)
def maintenance__janitor() -> list[str]:
    delete_stale_pending_runs(threshold_hours=1, batch_size=200)
