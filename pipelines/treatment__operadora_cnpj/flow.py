# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__operadora_cnpj import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__operadora_cnpj(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.OPERADORA_CNPJ_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        test_scheduled_time=None,
    )
