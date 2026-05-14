# -*- coding: utf-8 -*-
"""
Flow de apuração do subsídio SPPO

Migrado de pipelines_rj_smtr (Prefect 1.4) — `subsidio_sppo_apuracao`.
Executa selectors V8/V9/monitoramento conforme range de datas, snapshot final
e pré-checagem de captura de dados da Jaé.

Common: 2026-05-14
"""

from typing import Optional

from prefect import runtime, unmapped

from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
    task_send_discord_message,
)
from pipelines.common.treatment.default_treatment.flow import (
    create_materialization_flows_default_tasks,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.control__jae_verificacao_captura.tasks import (
    create_capture_check_discord_message,
    get_capture_gaps,
    jae_capture_check_get_ts_range,
)
from pipelines.treatment__subsidio_sppo_apuracao import constants
from pipelines.treatment__subsidio_sppo_apuracao.tasks import raise_on_gaps


@flow(log_prints=True, flow_run_name=rename_treatment_flow_run)
def treatment__subsidio_sppo_apuracao(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    additional_vars: Optional[dict] = None,
    fallback_run: bool = False,
    skip_source_check: bool = False,
    skip_pre_test: bool = False,
):
    tasks_wait_for = None

    if not skip_pre_test:
        env_task = get_run_env(env=env, deployment_name=runtime.deployment.name)
        sentry = initialize_sentry(env=env)
        setup_environment(env=env, wait_for=[sentry])
        timestamp = get_scheduled_timestamp(wait_for=[sentry])

        ts_start, ts_end = jae_capture_check_get_ts_range(
            timestamp=timestamp,
            retroactive_days=7,
            timestamp_captura_start=datetime_start,
            timestamp_captura_end=datetime_end,
        )

        gaps = get_capture_gaps.map(
            env=unmapped(env_task),
            table_id=constants.TABLE_IDS_JAE,
            timestamp_captura_start=unmapped(ts_start),
            timestamp_captura_end=unmapped(ts_end),
        ).result()

        discord_messages = create_capture_check_discord_message.map(
            table_id=constants.TABLE_IDS_JAE,
            timestamps=gaps,
            timestamp_captura_start=unmapped(ts_start),
            timestamp_captura_end=unmapped(ts_end),
        ).result()

        send_msg = task_send_discord_message(
            message=discord_messages,
            webhook=jae_constants.ALERT_WEBHOOK,
        )

        pre_test_jae = raise_on_gaps(
            gaps_per_table=gaps,
            table_ids=list(constants.TABLE_IDS_JAE),
            wait_for=[send_msg],
        )

        tasks_wait_for = {"pre_tests": [pre_test_jae]}

    create_materialization_flows_default_tasks(
        env=env,
        selectors=[
            constants.APURACAO_SUBSIDIO_V8_SELECTOR,
            constants.APURACAO_SUBSIDIO_V9_SELECTOR,
            constants.APURACAO_SUBSIDIO_V14_SELECTOR,
            constants.MONITORAMENTO_SUBSIDIO_SELECTOR,
        ],
        snapshot_selector=constants.SNAPSHOT_SUBSIDIO_SELECTOR,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        skip_source_check=skip_source_check,
        additional_vars={**constants.ADDITIONAL_VARS, **(additional_vars or {})},
        test_webhook_key=constants.WEBHOOK_KEY,
        tasks_wait_for=tasks_wait_for,
        fallback_run=fallback_run,
        skip_pre_test=skip_pre_test,
    )
