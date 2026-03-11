# -*- coding: utf-8 -*-
"""Tasks para pipeline control__source_freshness"""

import re
from typing import Optional

from prefect import task

from pipelines.common import constants as common_constants
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.secret import get_env_secret


@task(name="Parse Source Freshness Output")
def parse_source_freshness_output(dbt_output: str) -> tuple[bool, Optional[list[str]]]:
    """
    Analisa a saída do comando `dbt source freshness` para identificar fontes desatualizadas

    Args:
        dbt_output (str): Texto completo da saída do comando `dbt source freshness`

    Returns:
        tuple[bool, Optional[list[str]]]:
            - Um booleano que é True se existirem fontes com alerta (WARN), False caso contrário
            - Uma lista com os nomes das fontes desatualizadas ou uma lista vazia
    """
    failed_sources = re.findall(r"WARN freshness of ([\w\.]+)", dbt_output)

    return len(failed_sources) > 0, failed_sources


@task(name="Notify Discord - Source Freshness")
def source_freshness_notify_discord(failed_sources: list[str]):
    """
    Envia uma notificação para o Discord alertando sobre fontes de dados desatualizadas

    Args:
        failed_sources (list[str]): Lista com os nomes das fontes desatualizadas
    """
    webhook_url = get_env_secret(secret_path="webhooks")["dataplex"]
    mention_id = common_constants.OWNERS_DISCORD_MENTIONS["dados_smtr"]["user_id"]
    mentions_tag = f" - <@&{mention_id}>\n\n"
    formatted_messages = [f":red_circle: **Sources desatualizados** {mentions_tag}"]

    for source in failed_sources:
        formatted_messages.append(f":warning: {source}\n")

    format_send_discord_message(formatted_messages=formatted_messages, webhook_url=webhook_url)
