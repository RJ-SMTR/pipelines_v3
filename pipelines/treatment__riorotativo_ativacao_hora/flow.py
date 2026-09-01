# -*- coding: utf-8 -*-
from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__riorotativo_ativacao_hora import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__riorotativo_ativacao_hora(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.RIOROTATIVO_ATIVACAO_HORA_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
    )
