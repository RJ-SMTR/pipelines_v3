# -*- coding: utf-8 -*-
"""
Flow de materialização do select planejamento_diario

DBT: 2026-03-02
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__planejamento_diario import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__planejamento_diario(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.PLANEJAMENTO_DIARIO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
    )
