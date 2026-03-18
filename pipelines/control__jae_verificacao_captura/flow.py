# -*- coding: utf-8 -*-
"""
Flow de verificação da captura dos dados da Jaé

Common: 2026-03-18
"""

from typing import Iterable, Optional

from prefect import flow, runtime, unmapped

from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
    task_send_discord_message,
)
from pipelines.control__jae_verificacao_captura import constants
from pipelines.control__jae_verificacao_captura.tasks import (
    create_capture_check_discord_message,
    get_capture_gaps,
    jae_capture_check_get_ts_range,
)
from pipelines.control__jae_verificacao_captura.utils import rename_capture_check_flow_run

table_ids = constants.CHECK_CAPTURE_PARAMS.keys()


@flow(log_prints=True, flow_run_name=rename_capture_check_flow_run)
def control__jae_verificacao_captura(
    env: Optional[str] = None,
    timestamp_captura_start: Optional[str] = None,
    timestamp_captura_end: Optional[str] = None,
    table_ids: Iterable = tuple(table_ids),
    retroactive_days: int = 1,
):
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    sentry = initialize_sentry(env=env)
    setup_environment(env=env)

    timestamp = get_scheduled_timestamp(wait_for=[sentry])

    timestamp_captura_start, timestamp_captura_end = jae_capture_check_get_ts_range(
        timestamp=timestamp,
        retroactive_days=retroactive_days,
        timestamp_captura_start=timestamp_captura_start,
        timestamp_captura_end=timestamp_captura_end,
    )

    timestamps = get_capture_gaps.map(
        env=unmapped(env),
        table_id=table_ids,
        timestamp_captura_start=unmapped(timestamp_captura_start),
        timestamp_captura_end=unmapped(timestamp_captura_end),
    )

    discord_messages = create_capture_check_discord_message.map(
        table_id=table_ids,
        timestamps=timestamps,
        timestamp_captura_start=unmapped(timestamp_captura_start),
        timestamp_captura_end=unmapped(timestamp_captura_end),
    )

    task_send_discord_message.map(
        message=discord_messages,
        key=unmapped(jae_constants.ALERT_WEBHOOK),
    )
