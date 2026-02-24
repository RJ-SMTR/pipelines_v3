# -*- coding: utf-8 -*-
import os  # noqa: I001
from typing import Optional

from infisical_sdk import InfisicalSDKClient
from pipelines.common.utils.env import getenv_or_action


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


def set_local_secrets():
    """
    Define os secrets nas variáveis de ambiente com base no Infisical.
    """
    client = get_infisical_client()

    project_id = getenv_or_action("INFISICAL_PROJECT_ID", action="raise")

    for s in client.secrets.list_secrets(
        environment_slug="dev",
        secret_path="/",
        project_id=project_id,
    ).secrets:
        if not s.secretKey.startswith("BASEDOSDADOS_"):
            os.environ[s.secretKey] = s.secretValue


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
