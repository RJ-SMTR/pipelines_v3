# -*- coding: utf-8 -*-
from prefect import runtime

from pipelines.common import constants
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.secret import get_env_secret
from pipelines.common.utils.utils import convert_timezone


def handler_notify_failure(webhook: str):
    """Gera um state handler para notificar falhas no Discord.

    Args:
        webhook (str): A chave para acessar a URL do webhook no secret do infisical.

    Returns:
        Callable: O state handler
    """

    def handler(flow, flow_run, state):  # noqa: ARG001
        webhook_url = get_env_secret(secret_path=constants.WEBHOOKS_SECRET_PATH)[webhook]
        mentions_tag = f" - <@&{constants.OWNERS_DISCORD_MENTIONS['dados_smtr']['user_id']}>"
        header = f":red_circle: **Erro no flow {flow.name}**"
        header = f"{header} {mentions_tag}\n\n"

        formatted_messages = [header]
        flow_run_url = f"https://prefect.mobilidade.rio/runs/flow-run/{flow_run.id}"

        formatted_messages.append(f"**URL da execução:** {flow_run_url}")
        format_send_discord_message(formatted_messages=formatted_messages, webhook_url=webhook_url)

    return handler


def rename_flow_run() -> str:
    """
    Gera o nome para execução de flows de tratamento.

    Returns:
        str: Nome para execução do flow.
    """
    scheduled_start_time = convert_timezone(runtime.flow_run.scheduled_start_time).strftime(
        "%Y-%m-%d %H-%M-%S"
    )

    flow_name = runtime.flow_run.flow_name
    return f"[{scheduled_start_time}] {flow_name}"
