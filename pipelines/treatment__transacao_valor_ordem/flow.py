# -*- coding: utf-8 -*-
from typing import Optional

from prefect import flow

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import handler_notify_failure
from pipelines.treatment__transacao_valor_ordem import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def treatment__transacao_valor_ordem(
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    force_test_run: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.TRANSACAO_VALOR_ORDEM_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        test_scheduled_time=None,
        force_test_run=force_test_run,
        test_webhook_key="alertas_bilhetagem",
    )
