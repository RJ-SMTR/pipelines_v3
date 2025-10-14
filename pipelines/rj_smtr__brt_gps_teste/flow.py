# -*- coding: utf-8 -*-
import random

import pandas as pd
from prefect import flow, task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task

from constants import constants
from tasks import (
    pre_treatment_br_rj_riodejaneiro_brt_gps,
)
from tasks import (  # get_local_dbt_client,; setup_task,
    bq_upload,
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_now_time,
    get_raw,
    parse_timestamp_to_string,
    # rename_current_flow_run_now_time,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
)


@task
def use_pandas():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    print(df)


@task
def get_customer_ids() -> list[str]:
    return [f"customer{n}" for n in random.choices(range(100), k=10)]


@task
def process_customer(customer_id: str) -> str:
    return f"Processed {customer_id}"


@flow(log_prints=True)
def rj_smtr__gps_brt() -> list[str]:
    inject_bd_credentials_task()

    timestamp = get_current_timestamp()
    # Rename flow run
    # rename_flow_run = rename_current_flow_run_now_time(
    #     prefix=rj_smtr__gps_brt.name + ": ", now_time=timestamp
    # )

    # SETUP LOCAL #
    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_BRT_RAW_DATASET_ID.value,
        table_id=constants.GPS_BRT_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )
    # EXTRACT

    raw_status = get_raw(
        url=constants.GPS_BRT_API_URL.value,
        headers=constants.GPS_BRT_API_SECRET_PATH.value,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)
    # TREAT
    treated_status = pre_treatment_br_rj_riodejaneiro_brt_gps(
        status=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)
    # LOAD
    error = bq_upload(
        dataset_id=constants.GPS_BRT_RAW_DATASET_ID.value,
        table_id=constants.GPS_BRT_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.GPS_BRT_RAW_DATASET_ID.value,
        parent_table_id=constants.GPS_BRT_RAW_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    # rj_smtr__gps_brt.set_dependencies(task=rename_flow_run, upstream_tasks=[timestamp])
    # rj_smtr__gps_brt.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])
