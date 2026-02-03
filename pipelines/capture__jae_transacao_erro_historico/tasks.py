# -*- coding: utf-8 -*-
from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import task

from pipelines.common import constants
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.gcp.storage import Storage
from pipelines.common.utils.utils import convert_timezone, cron_date_range


@task
def get_transacao_erro_timestamps(source: SourceTable) -> list[str]:
    file_length = 23
    prefix = f"source/{source.dataset_id}/{source.table_id}/data="
    full_range = cron_date_range(
        cron_expr=source.schedule_cron,
        start_time=datetime(2023, 10, 12, 0, 0, 0, tzinfo=ZoneInfo(constants.TIMEZONE)),
        end_time=datetime(2024, 3, 18, 0, 0, 0, tzinfo=ZoneInfo(constants.TIMEZONE)),
    )
    st = Storage(
        env="prod",
        dataset_id=source.dataset_id,
        table_id=source.table_id,
        bucket_names=source.bucket_names,
    )
    files = [
        convert_timezone(
            datetime.strptime(b.name.split("/")[-1], "%Y-%m-%d-%H-%M-%S.csv").replace(
                tzinfo=ZoneInfo(constants.TIMEZONE)
            )
        )
        for b in st.bucket.list_blobs(prefix=prefix)
        if ".csv" in b.name and len(b.name.split("/")[-1]) == file_length
    ]

    return [d.strftime("%Y-%m-%d %H:%M:%S") for d in full_range if d not in files].sort()[::-1][:24]
