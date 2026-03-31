# -*- coding: utf-8 -*-
from datetime import time

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.treatment__transacao import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def treatment__transacao(
    env=None,
    datetime_start=None,
    datetime_end=None,
    flags=None,
    force_test_run=False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.TRANSACAO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        test_scheduled_time=time(12, 15, 0),
        force_test_run=force_test_run,
    )
