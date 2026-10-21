# -*- coding: utf-8 -*-
from prefect import flow as prefect_flow
from prefect import get_client, runtime
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.states import get_state_exception

from pipelines.common import constants
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.secret import get_env_secret
from pipelines.common.utils.utils import convert_timezone


def flow(*args, timeout_seconds=constants.DEFAULT_FLOW_TIMEOUT, **kwargs):
    """
    Substitui o @flow do Prefect aplicando timeout padrão em todos os flows.
    """
    return prefect_flow(*args, timeout_seconds=timeout_seconds, **kwargs)


def handler_notify_failure(webhook: str):
    """Gera um state handler para notificar falhas no Discord.

    Args:
        webhook (str): A chave para acessar a URL do webhook no secret do infisical.

    Returns:
        Callable: O state handler
    """

    def handler(flow, flow_run, state):
        webhook_url = get_env_secret(secret_path=constants.WEBHOOKS_SECRET_PATH)[webhook]
        mentions_tag = f" - <@&{constants.OWNERS_DISCORD_MENTIONS['dados_smtr']['user_id']}>"
        header = f":red_circle: **Erro no flow {flow.name}**"
        exc = get_state_exception(state)
        flow_run_url = f"https://prefect-v3.mobilidade.rio/runs/flow-run/{flow_run.id}"

        with get_client(sync_client=True) as client:
            task_runs = client.read_task_runs(
                flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[flow_run.id]))
            )

        failed_tasks = "\n".join(
            [
                f"\t{t.name}"
                for t in task_runs
                if t.state and (t.state.is_failed() or t.state.is_crashed())
            ]
        )

        format_send_discord_message(
            formatted_messages=[
                f"{header} {mentions_tag}\n\n",
                f"** As seguintes tasks falharam:**\n{failed_tasks}\n\n"
                f"**{type(exc).__name__}:** {exc}\n\n",
                f"**URL da execução:** {flow_run_url}",
            ],
            webhook_url=webhook_url,
        )

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


class FailedSubFlowError(Exception):
    """Erro para ser usado quando um subflow falha"""
