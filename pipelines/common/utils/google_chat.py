# -*- coding: utf-8 -*-
"""Envio de mensagens para espaços do Google Chat via webhook"""

import time
from typing import Optional

import requests
from prefect import runtime

from pipelines.common import constants
from pipelines.common.utils.secret import get_env_secret


def send_google_chat_message(text: str, webhook_url: str, thread_key: str | None = None) -> None:
    """
    Envia uma mensagem de texto para um espaço do Google Chat.

    Args:
        text (str): Texto da mensagem, na sintaxe de formatação do Google Chat.
        webhook_url (str): URL do webhook do espaço.
        thread_key (str | None): Chave da thread; mensagens com a mesma chave ficam agrupadas.

    Raises:
        requests.HTTPError: O Google Chat recusou a mensagem.
    """
    body = {"text": text}
    params = {}
    if thread_key is not None:
        body["thread"] = {"threadKey": thread_key}
        params["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
    response = requests.post(
        webhook_url,
        json=body,
        params=params,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        timeout=30,
    )
    response.raise_for_status()


def split_google_chat_message(lines: list[str]) -> list[str]:
    """
    Agrupa as linhas em mensagens dentro do limite de tamanho do Google Chat.

    Args:
        lines (list[str]): Linhas da mensagem, sem quebra de linha final.

    Returns:
        list[str]: Mensagens com até GOOGLE_CHAT_MAX_MESSAGE_BYTES bytes em UTF-8.
    """
    messages = []
    chunk = ""
    for line in lines:
        line_size = len(f"{line}\n".encode())
        if chunk and len(chunk.encode()) + line_size > constants.GOOGLE_CHAT_MAX_MESSAGE_BYTES:
            messages.append(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        messages.append(chunk)
    return messages


def format_send_google_chat_message(
    lines: list[str], webhook_url: str, thread_key: str | None = None
) -> None:
    """
    Divide a mensagem respeitando o limite de tamanho e envia cada parte na mesma thread.

    Args:
        lines (list[str]): Linhas da mensagem.
        webhook_url (str): URL do webhook do espaço.
        thread_key (str | None): Chave da thread das mensagens.
    """
    for position, message in enumerate(split_google_chat_message(lines)):
        if position:
            # cota de 1 requisição por segundo por espaço
            time.sleep(1.1)
        send_google_chat_message(text=message, webhook_url=webhook_url, thread_key=thread_key)


def format_test_failures_message(title: str, failures: list[dict]) -> list[str]:
    """
    Monta as linhas da mensagem com os testes que falharam, agrupados por tabela.

    Args:
        title (str): Título da mensagem.
        failures (list[dict]): Testes com falha, com as chaves table e description.

    Returns:
        list[str]: Linhas da mensagem na sintaxe do Google Chat.
    """
    lines = [f"🔴 *{title}*"]
    current_table = None
    for failure in failures:
        if failure["table"] != current_table:
            current_table = failure["table"]
            lines.extend(["", f"*{current_table}*"])
        lines.append(f"❌ {failure['description']}")
    return lines


def notify_test_results_google_chat(
    results: list[dict],
    title: str,
    env: str,
    webhook_key: Optional[str],
) -> None:
    """
    Envia os testes com falha (contrato de dados ou dbt) para um espaço do Google Chat.

    Só há mensagem quando algum teste falhou; avisos não são notificados. A mensagem é sempre
    impressa no log, mas só é enviada em prod. Falhas no envio não interrompem o flow.

    Args:
        results (list[dict]): Resultados com as chaves table, description e result
            (PASS, WARN, FAIL ou ERROR).
        title (str): Título da mensagem.
        env (str): prod ou dev.
        webhook_key (Optional[str]): Chave do webhook no secret; sem chave, não envia.
    """
    failures = [result for result in results if result["result"] not in ("PASS", "WARN")]
    if not failures:
        return
    lines = format_test_failures_message(title=title, failures=failures)
    print("\n".join(lines))
    if env != "prod":
        print("Ambiente dev: mensagem não enviada ao Google Chat.")
        return
    if not webhook_key:
        print("Sem webhook do Google Chat configurado; mensagem não enviada.")
        return
    try:
        webhook_url = get_env_secret(constants.WEBHOOKS_SECRET_PATH)[webhook_key]
        format_send_google_chat_message(
            lines=lines, webhook_url=webhook_url, thread_key=str(runtime.flow_run.id)
        )
    except Exception as error:
        print(f"Falha ao enviar mensagem para o Google Chat: {error}")
