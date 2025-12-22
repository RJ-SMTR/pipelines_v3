# -*- coding: utf-8 -*-
"""
Tasks para integração com a API da Previnity
"""

from datetime import date

from prefect import task

from pipelines.common.utils.pretreatment import normalize_text


@task
def prepare_previnity_payloads(data: list[dict], execution_date: date) -> list[dict]:
    """
    Prepara os payloads para envio à API da Previnity, separando inclusão (controle=1)
    e baixa (controle=2) com base na data de execução.

    Args:
        data (list[dict]): Dados retornados da query.
        execution_date (date): Data de referência para inclusão/baixa.

    Returns:
        list[dict]: Lista de payloads formatados.
    """
    payloads = []

    for row in data:
        dt_inclusao = row.get("data_inclusao")
        dt_baixa = row.get("data_baixa")

        common_body = {
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

        if dt_inclusao == execution_date:
            payload = common_body.copy()
            payload["controle"] = "1"
            payloads.append(payload)

        if dt_baixa == execution_date:
            payload = common_body.copy()
            payload["controle"] = "2"
            payloads.append(payload)

    return payloads
