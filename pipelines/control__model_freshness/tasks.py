# -*- coding: utf-8 -*-
"""Tasks para pipeline control__model_freshness"""

from typing import Optional

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as common_constants
from pipelines.common.treatment.default_treatment.utils import (
    extract_relation_from_query,
    parse_dbt_test_output,
)
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE, name="Parse Model Freshness Output")
def parse_model_freshness_output(dbt_output: str) -> tuple[bool, Optional[dict]]:
    """
    Reaproveita o parser dos testes do dbt para extrair tabelas desatualizadas.

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
def model_freshness_notify_discord(failed_results: dict, test_select: str):
    """
    Envia notificação ao Discord listando tabelas desatualizadas.

    Args:
        failed_results (dict): Resultados retornados por `parse_model_freshness_output`.
        test_select (str): Selector dbt usado na execução (ex.: "tag:hourly").
    """
    webhook_url = get_env_secret(secret_path=common_constants.WEBHOOKS_SECRET_PATH)["dataplex"]
    mention_id = common_constants.OWNERS_DISCORD_MENTIONS["dados_smtr"]["user_id"]
    mentions_tag = f" - <@&{mention_id}>\n"
    tag_label = ", ".join(selector.removeprefix("tag:") for selector in test_select.split())
    formatted_messages = [f":warning: **Tabelas desatualizadas** [`{tag_label}`]{mentions_tag}"]

    for test_name, info in failed_results.items():
        relation = extract_relation_from_query(info.get("query"))
        if relation is None:
            parts = test_name.split("__")
            if len(parts) >= 3:  # noqa: PLR2004
                relation = parts[2]
            elif len(parts) == 2:  # noqa: PLR2004
                relation = parts[1]
            else:
                relation = test_name

        formatted_messages.append(f" - `{relation}`\n")

    format_send_discord_message(formatted_messages=formatted_messages, webhook_url=webhook_url)
