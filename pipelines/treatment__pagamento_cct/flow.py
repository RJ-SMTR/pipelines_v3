# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__pagamento_cct import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__pagamento_cct(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict[str, str]] = None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.PAGAMENTO_CCT_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
    )
