# -*- coding: utf-8 -*-
"""Funções do flow treatment__subsidio_sppo_apuracao"""

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
from pipelines.control__jae_verificacao_captura.tasks import (
    create_capture_check_discord_message,
    get_capture_gaps,
    jae_capture_check_get_ts_range,
)
from pipelines.treatment__subsidio_sppo_apuracao.tasks import raise_on_gaps


def wait_jae_capture_gaps(  # noqa: PLR0913
    env: Optional[str],
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    table_ids: list[str],
    retroactive_days: int = 7,
    webhook: str = jae_constants.ALERT_WEBHOOK,
):
    """
    Executa a pré-checagem de captura da Jaé e retorna a task de bloqueio.

    Verifica gaps de captura nas tabelas da Jaé, notifica no Discord e bloqueia
    a materialização (`raise_on_gaps`) caso haja gaps. Deve ser chamada dentro de
    um `@flow`; o futuro retornado serve de gate via `tasks_wait_for`.

    Args:
        env (Optional[str]): prod ou dev. Resolvido via `get_run_env`.
        datetime_start (Optional[str]): Início forçado da janela de captura.
        datetime_end (Optional[str]): Fim forçado da janela de captura.
        table_ids (list[str]): table_ids da Jaé a verificar.
        retroactive_days (int): Dias retroativos quando datetime não é forçado.
        webhook (str): Key do webhook do Discord para o alerta.

    Returns:
        A task `raise_on_gaps` (gate para `tasks_wait_for`).
    """
    env_task = get_run_env(env=env, deployment_name=runtime.deployment.name)
    sentry = initialize_sentry(env=env)
    setup_environment(env=env, wait_for=[sentry])
    timestamp = get_scheduled_timestamp(wait_for=[sentry])

    ts_start, ts_end = jae_capture_check_get_ts_range(
        timestamp=timestamp,
        retroactive_days=retroactive_days,
        timestamp_captura_start=datetime_start,
        timestamp_captura_end=datetime_end,
    )

    gaps = get_capture_gaps.map(
        env=unmapped(env_task),
        table_id=table_ids,
        timestamp_captura_start=unmapped(ts_start),
        timestamp_captura_end=unmapped(ts_end),
    ).result()

    discord_messages = create_capture_check_discord_message.map(
        table_id=table_ids,
        timestamps=gaps,
        timestamp_captura_start=unmapped(ts_start),
        timestamp_captura_end=unmapped(ts_end),
    ).result()

    send_msg = task_send_discord_message(
        message=discord_messages,
        webhook=webhook,
    )

    return raise_on_gaps(
        gaps_per_table=gaps,
        table_ids=list(table_ids),
        wait_for=[send_msg],
    )
