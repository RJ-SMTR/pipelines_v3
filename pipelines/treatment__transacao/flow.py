# -*- coding: utf-8 -*-
"""
Flow de materialização dos dados de transação

DBT: 2026-05-15
"""

from datetime import time
from typing import Optional

from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import (
    MaterializationTestConfig,
    rename_treatment_flow_run,
)
from pipelines.common.utils.prefect import flow, handler_notify_failure
from pipelines.treatment__transacao import constants


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
    on_failure=[handler_notify_failure(webhook="alertas_bilhetagem")],
    on_crashed=[handler_notify_failure(webhook="alertas_bilhetagem")],
)
def treatment__transacao(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    force_test_run: bool = False,
    skip_source_check: bool = False,
):
    create_materialization_flows_default_tasks(
        env=env,
        selectors=[constants.TRANSACAO_SELECTOR],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        flags=flags,
        additional_vars=additional_vars,
        test_config=MaterializationTestConfig(
            scheduled_time=time(12, 15, 0),
            force_run=force_test_run,
            webhook_key="alertas_bilhetagem",
        ),
        skip_source_check=skip_source_check,
    )
