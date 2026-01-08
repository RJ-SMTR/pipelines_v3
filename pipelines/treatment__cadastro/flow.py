# -*- coding: utf-8 -*-
"""
DBT: 2026-01-08
"""

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__cadastro(  # noqa: PLR0913
    env,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=None,
        force_test_run=force_test_run,
    )
