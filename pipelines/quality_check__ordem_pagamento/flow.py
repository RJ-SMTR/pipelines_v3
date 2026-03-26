# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_quality_check.flow import (
    create_quality_check_flows_default_tasks,
)
from pipelines.quality_check__ordem_pagamento import constants


@flow(log_prints=True)
def quality_check__ordem_pagamento(
    env: Optional[str],
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    partitions: Optional[str],
) -> list[str]:
    create_quality_check_flows_default_tasks(
        env=env,
        dbt_test=constants.ORDEM_PAGAMENTO_DBT_TEST,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        partitions=partitions,
        webhook_key=constants.ORDEM_PAGAMENTO_ALERT_WEBHOOK,
        additional_mentions="devs_smtr",
    )
