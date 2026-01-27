# -*- coding: utf-8 -*-
"""
Tasks para integração com a API da Previnity
"""

from datetime import date, datetime
from typing import Optional

import pandas as pd
from prefect import task

from pipelines.common.utils.pretreatment import normalize_text
from pipelines.common.utils.secret import get_secret
from pipelines.common.utils.utils import convert_timezone
from pipelines.integration__previnity_negativacao import constants
from pipelines.treatment__transito_autuacao.constants import TRANSITO_AUTUACAO_SELECTOR


@task
def get_previnity_credentials() -> tuple[str, str]:
    """
    Retorna as credenciais para autenticação na API da Previnity.
    """
    prev_key = get_secret(secret_path="previnity_api", secret_name="key").get("key")
    prev_token = get_secret(secret_path="previnity_api", secret_name="token").get("token")

    if not prev_key or not prev_token:
        raise ValueError("Missing 'prev_key' or 'prev_token' in secrets.")

    return prev_key, prev_token


@task
def prepare_previnity_payloads(
    data: list[dict], datetime_start: date, datetime_end: date
) -> list[tuple[dict, dict]]:
    """
    Prepara os payloads para envio à API da Previnity, separando inclusão (controle=1)
    e baixa (controle=2) com base na data de execução.

    Args:
        data (list[dict]): Dados retornados da query.
        datetime_start (date): Data de início do intervalo.
        datetime_end (date): Data de fim do intervalo.

    Returns:
        list[tuple[dict, dict]]: Lista de (payload, metadata).
    """
    payloads = []

    for row in data:
        dt_inclusao = row.get("data_inclusao")
        dt_baixa = row.get("data_baixa")

        is_inclusao_in_range = (
            pd.notna(dt_inclusao) and datetime_start <= dt_inclusao <= datetime_end
        )
        is_baixa_in_range = pd.notna(dt_baixa) and datetime_start <= dt_baixa <= datetime_end

        if not is_inclusao_in_range and not is_baixa_in_range:
            continue

        body = {
            "controle": "2" if is_baixa_in_range else "1",
            "nome": normalize_text(row.get("nome")),
            "cpf": normalize_text(row.get("cpf")),
            "endereco": normalize_text(row.get("endereco")),
            "bairro": normalize_text(row.get("bairro")),
            "cidade": normalize_text(row.get("cidade")),
            "cep": normalize_text(row.get("cep")),
            "estado": normalize_text(row.get("estado")),
            "contrato": normalize_text(row.get("contrato")),
            "datavencimento": row.get("datavencimento"),
            "datavenda": row.get("datavenda"),
            "valor": row.get("valor"),
            "webservice": "S",
        }

        metadata = {
            "data_autuacao": row.get("data"),
        }

        payloads.append((body, metadata))

    return payloads


@task
def get_previnity_datetime_start(env: str) -> datetime:
    """
    Retorna o datetime de início para a materialização da negativacao.
    """
    autuacao_last_materialization = TRANSITO_AUTUACAO_SELECTOR.get_last_materialized_datetime(
        env=env
    )
    negativacao_last_materialization = (
        constants.NEGATIVACAO_SELECTOR.get_last_materialized_datetime(env=env)
    )

    min_last_materialization = min(autuacao_last_materialization, negativacao_last_materialization)

    return max(
        min_last_materialization,
        constants.NEGATIVACAO_SELECTOR.initial_datetime,
    )
