# -*- coding: utf-8 -*-
from prefect import flow
from tasks import delete_old_flow_runs


@flow(log_prints=True)
def maintenance__db_retention():
    delete_old_flow_runs(days_to_keep=15, batch_size=200)
