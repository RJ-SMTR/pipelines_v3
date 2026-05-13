# -*- coding: utf-8 -*-
"""Tasks para pipeline control__model_freshness"""

from typing import Optional

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as common_constants
from pipelines.common.treatment.default_treatment.utils import parse_dbt_test_output
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE, name="Parse Model Freshness Output")
def parse_model_freshness_output(dbt_output: str) -> tuple[bool, Optional[dict]]:
    """
    Reaproveita o parser dos testes do dbt para extrair modelos com falha de freshness.

    Args:
        dbt_output (str): Logs do `dbt test` em formato JSON.

    Returns:
        tuple[bool, Optional[dict]]:
            - Booleano indicando se há resultados FAIL/WARN/ERROR.
            - Dicionário {test_name: {"result": ..., ...}} contendo apenas falhas.
    """
    results = parse_dbt_test_output(dbt_output)
    failed = {name: info for name, info in results.items() if info["result"] in ("FAIL", "WARN")}
    return len(failed) > 0, failed


@task(cache_policy=NO_CACHE, name="Notify Discord - Model Freshness")
def model_freshness_notify_discord(failed_results: dict):
    """
    Envia notificação ao Discord listando modelos com anomalia de freshness.

    Args:
        failed_results (dict): Resultados retornados por `parse_model_freshness_output`.
    """
    webhook_url = get_env_secret(secret_path=common_constants.WEBHOOKS_SECRET_PATH)["dataplex"]
    mention_id = common_constants.OWNERS_DISCORD_MENTIONS["dados_smtr"]["user_id"]
    mentions_tag = f" - <@&{mention_id}>\n\n"
    formatted_messages = [f":red_circle: **Modelos com anomalia de freshness** {mentions_tag}"]

    emoji_by_result = {"WARN": ":warning:", "FAIL": ":x:"}

    for test_name, info in failed_results.items():
        parts = test_name.split("__")
        if len(parts) >= 3:  # noqa: PLR2004
            table_name = parts[2]
        elif len(parts) == 2:  # noqa: PLR2004
            table_name = parts[1]
        else:
            table_name = test_name

        emoji = emoji_by_result[info["result"]]
        formatted_messages.append(f"{emoji} `{table_name}` ({info['result']})\n")

    format_send_discord_message(formatted_messages=formatted_messages, webhook_url=webhook_url)
