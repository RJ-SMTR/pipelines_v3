# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from prefect import flow, runtime

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.common.utils.utils import convert_timezone
from pipelines.treatment__sppo_viagens import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def treatment__sppo_viagens(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    additional_vars=None,
    fallback_run=False,
    skip_source_check=False,
):
    scheduled_time = convert_timezone(runtime.flow_run.scheduled_start_time)
    second_run = scheduled_time.hour >= 14 and datetime_start is None and datetime_end is None

    if second_run:
        d1 = (scheduled_time + timedelta(days=1)).date()
        datetime_start = f"{d1}T00:00:00"
        datetime_end = f"{d1}T23:59:59"

    selector = constants.VIAGENS_SPPO_D0_SELECTOR if second_run else constants.VIAGENS_SPPO_SELECTOR
    snapshot_selector = None if second_run else constants.VIAGENS_SPPO_SNAPSHOT_SELECTOR

    if datetime_start and datetime_end:
        start = datetime.fromisoformat(datetime_start).date()
        end = datetime.fromisoformat(datetime_end).date()

        run_dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        run_dates = [None]

    for run_date in run_dates:
        if run_date is not None:
            run_date_str = run_date.strftime("%Y-%m-%d")
            iter_start = f"{run_date_str}T00:00:00"
            iter_end = f"{run_date_str}T23:59:59"
            iter_vars = {**(additional_vars or {}), "run_date": run_date_str}
        else:
            iter_start = datetime_start
            iter_end = datetime_end
            iter_vars = additional_vars

        create_materialization_flows_default_tasks(
            env=env,
            selectors=[selector],
            datetime_start=iter_start,
            datetime_end=iter_end,
            flags=flags,
            additional_vars=iter_vars,
            test_scheduled_time=None,
            fallback_run=fallback_run,
            skip_source_check=skip_source_check,
            snapshot_selector=snapshot_selector,
        )
