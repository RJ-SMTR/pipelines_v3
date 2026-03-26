# -*- coding: utf-8 -*-
"""
Flow para o teste de qualidade da ordem de pagamento da Jaé
"""

from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_quality_check.flow import (
    create_quality_check_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.quality_check__ordem_pagamento import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def quality_check__ordem_pagamento(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    partitions: Optional[str] = None,
) -> list[str]:
    create_quality_check_flows_default_tasks(
        env=env,
        dbt_test=constants.ORDEM_PAGAMENTO_DBT_TEST,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        partitions=partitions,
        webhook_key=constants.ORDEM_PAGAMENTO_ALERT_WEBHOOK,
        additional_mentions=["devs_smtr"],
    )
