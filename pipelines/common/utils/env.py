# -*- coding: utf-8 -*-
import base64
from os import environ, getenv
from pathlib import Path
from typing import Optional, Union


def getenv_or_action(
    key: str, default: Optional[str] = None, action: str = "raise"
) -> Union[str, None]:
    """
    Obtém uma variável de ambiente ou executa uma ação caso ela não exista.

    Args:
        key (str): Nome da variável de ambiente.
        default (str, opcional): Valor padrão caso a variável não exista.
            O padrão é `None`.
        action (str, opcional): Ação a ser executada caso a variável não exista.
            Deve ser uma de `'raise'`, `'warn'` ou `'ignore'`.
            O padrão é `'raise'`.

    Returns:
        Union[str, None]: O valor da variável de ambiente ou o valor padrão.
    """
    if action not in ("raise", "warn", "ignore"):
        raise ValueError(f"Ação inválida '{action}'. Deve ser uma de 'raise', 'warn' ou 'ignore'.")
    value = getenv(key, default)
    if value is None:
        if action == "raise":
            raise ValueError(f"Variável de ambiente '{key}' não encontrada.")
        elif action == "warn":
            print(f"Aviso: variável de ambiente '{key}' não encontrada.")
    return value


def validate_bd_credentials():
    """
    Valida se as variáveis de ambiente obrigatórias de credenciais da
    Base dos Dados estão definidas.
    """
    creds_name = [
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
        "BASEDOSDADOS_CONFIG",
    ]

    for cred_name in creds_name:
        _ = getenv_or_action(cred_name, action="raise")


def inject_bd_credentials(environment: str = "prod"):
    """
    Injeta as credenciais da Base dos Dados no ambiente, criando um arquivo
    temporário de credenciais e configurando a variável
    GOOGLE_APPLICATION_CREDENTIALS.

    Args:
        environment (str): Ambiente das credenciais (`prod` ou `staging`).
    """
    validate_bd_credentials()
    service_account_name = f"BASEDOSDADOS_CREDENTIALS_{environment.upper()}"
    service_account_b64 = getenv_or_action(service_account_name)
    if service_account_b64:
        service_account = base64.b64decode(service_account_b64)
    else:
        raise ValueError(f"{service_account_name} env var not set!")

    with Path("/tmp/credentials.json").open("wb") as credentials_file:
        credentials_file.write(service_account)
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"
    print(f"INJECTED: {service_account_name}")
