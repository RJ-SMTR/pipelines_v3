# -*- coding: utf-8 -*-
"""
Common 2026-02-09
"""

from datetime import time

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__gps_validador import constants


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__gps_validador(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.GPS_VALIDADOR_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_scheduled_time=time(1, 15, 0),
        force_test_run=force_test_run,
    )
