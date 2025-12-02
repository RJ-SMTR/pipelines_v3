# -*- coding: utf-8 -*-
import os
from typing import Optional

from infisical_sdk import InfisicalSDKClient
from iplanrio.pipelines_utils.env import getenv_or_action

from pipelines.common.utils.utils import is_running_locally


def get_infisical_client() -> InfisicalSDKClient:
    """
    Returns an Infisical client using the default settings from environment variables.

    Returns:
        InfisicalSDKClient: The Infisical client.
    """
    client_id = getenv_or_action("INFISICAL_CLIENT_ID", action="raise")
    client_secret = getenv_or_action("INFISICAL_CLIENT_SECRET", action="raise")
    site_url = getenv_or_action("INFISICAL_ADDRESS", action="raise")
    client = InfisicalSDKClient(host=site_url)
    client.auth.universal_auth.login(
        client_id=client_id,
        client_secret=client_secret,
    )
    return client


def get_infisical_secret(
    secret_path: str,
    secret_name: Optional[str] = None,
) -> dict:
    """
    Pega os dados de um secret no Infisical. Se for passado somente um secret_path
    sem o argumento secret_name, retorna todos os secrets dentro da pasta.

    Args:
        secret_path (str): Pasta do secret no infisical.
        secret_name (str, optional): Nome do secret. Defaults to None.

    Returns:
        dict: Dicionário com os dados retornados do Infisical
    """
    client = get_infisical_client()

    project_id = getenv_or_action("INFISICAL_PROJECT_ID", action="raise")

    prefix = f"{secret_path}_"
    if secret_name:
        secret = client.secrets.get_secret_by_name(
            secret_name=prefix + secret_name,
            environment_slug="dev",
            project_id=project_id,
            secret_path="/",
        )
        return {secret.secretKey[len(prefix) :].lower(): secret.secretValue}

    return {
        s.secretKey[len(prefix) :]: s.secretValue
        for s in client.secrets.list_secrets(
            environment_slug="dev",
            secret_path="/",
            project_id=project_id,
        ).secrets
        if s.secretKey.startswith(prefix)
    }


def get_env_secret(
    secret_path: str,
    secret_name: Optional[str] = None,
) -> dict:
    """
    Obtém segredos a partir de variáveis de ambiente usando um prefixo padrão.

    Args:
        secret_path (str): Caminho base usado como prefixo das variáveis de ambiente.
        secret_name (Optional[str]): Nome específico do segredo a ser retornado.
            Se não fornecido, retorna todos os segredos com o prefixo.

    Returns:
        dict: Dicionário com o(s) segredo(s) encontrado(s).
    """
    prefix = f"{secret_path}_"
    if secret_name:
        key = f"{prefix}{secret_name}"
        value = os.getenv(key)
        if value is None:
            raise KeyError(f"Variável de ambiente '{key}' não encontrada.")
        return {secret_name.lower(): value}

    secrets = {k[len(prefix) :].lower(): v for k, v in os.environ.items() if k.startswith(prefix)}

    if not secrets:
        raise KeyError(f"Nenhuma variável encontrada com prefixo '{prefix}'.")
    return secrets


def get_secret(
    secret_path: str,
    secret_name: Optional[str] = None,
) -> dict:
    """
    Obtém segredos do ambiente atual, usando Infisical quando local
        ou variáveis de ambiente quando remoto.

    Args:
        secret_path (str): Caminho base usado para identificar o segredo.
        secret_name (Optional[str]): Nome específico do segredo.

    Returns:
        dict: Dicionário com o(s) segredo(s) obtido(s).
    """
    if is_running_locally():
        return get_infisical_secret(secret_path=secret_path, secret_name=secret_name)
    return get_env_secret(secret_path=secret_path, secret_name=secret_name)
