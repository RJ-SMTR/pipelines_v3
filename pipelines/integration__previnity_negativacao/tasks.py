# -*- coding: utf-8 -*-
"""
Tasks para integração com a API da Previnity
"""

from datetime import date

from prefect import task

from pipelines.common.utils.pretreatment import normalize_text
from pipelines.common.utils.secret import get_secret


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
def prepare_previnity_payloads(data: list[dict], execution_date: date) -> list[tuple[dict, dict]]:
    """
    Prepara os payloads para envio à API da Previnity, separando inclusão (controle=1)
    e baixa (controle=2) com base na data de execução.

    Args:
        data (list[dict]): Dados retornados da query.
        execution_date (date): Data de referência para inclusão/baixa.

    Returns:
        list[tuple[dict, dict]]: Lista de (payload, metadata).
    """
    payloads = []

    for row in data:
        dt_inclusao = row.get("data_inclusao")
        dt_baixa = row.get("data_baixa")

        if dt_inclusao != execution_date and dt_baixa != execution_date:
            continue

        body = {
            "controle": "1" if dt_inclusao == execution_date else "2",
            "nome": normalize_text(row.get("nome")),
            "cpf": normalize_text(row.get("cpf")),
            "endereco": normalize_text(row.get("endereco")),
            "bairro": normalize_text(row.get("bairro")),
            "cidade": normalize_text(row.get("cidade")),
            "cep": normalize_text(row.get("cep")),
            "estado": normalize_text(row.get("estado")),
            "contrato": normalize_text(row.get("contrato")),
            "datavencimento": row.get("datavencimento", ""),
            "datavenda": row.get("datavenda", ""),
            "valor": row.get("valor", ""),
            "webservice": row.get("webservice", "S"),
        }

        metadata = {
            "data_autuacao": row.get("data"),
        }

        payloads.append((body, metadata))

    return payloads
