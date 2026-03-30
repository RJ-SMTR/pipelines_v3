# -*- coding: utf-8 -*-
"""
Tasks para captura de registro de agentes de verão de veículos SPPO
"""

from typing import Callable

import pandas as pd

from pipelines.capture__veiculo_registro_agente_verao import constants
from pipelines.common.utils.extractors.api import get_api_data
from pipelines.common.utils.secret import get_env_secret


def create_api_extractor_task() -> Callable:
    """
    Cria a função extratora de dados da API de agentes de verão.

    Returns:
        Callable: Função que extrai dados da API
    """

    def extract_from_api(timestamp=None, **kwargs):  # noqa: ARG001
        """
        Extrai dados da API de agentes de verão.

        Args:
            timestamp: Timestamp da captura (não utilizado)
            **kwargs: Argumentos adicionais (não utilizados)

        Returns:
            dict: Status da extração com os dados ou erro
        """
        try:
            # Obter credenciais da API
            api_credentials = get_env_secret(constants.AGENTES_VERAO_SECRET_PATH)

            if not api_credentials:
                return {
                    "status": "error",
                    "error": "API credentials not found",
                }

            # Extrair dados da API
            api_url = api_credentials.get("url")
            if not api_url:
                return {
                    "status": "error",
                    "error": "API URL not configured",
                }

            headers = {
                "Authorization": f"Bearer {api_credentials.get('token', '')}",
                "Content-Type": "application/json",
            }

            response_data = get_api_data(
                url=api_url,
                headers=headers,
                raw_filetype="json",
            )

            if not response_data:
                return {
                    "status": "warning",
                    "error": "No data returned from API",
                }

            # Converter lista de dicts para DataFrame
            if isinstance(response_data, list):
                df = pd.DataFrame(response_data)
            elif isinstance(response_data, dict):
                df = pd.DataFrame([response_data])
            else:
                return {
                    "status": "error",
                    "error": f"Unexpected data format: {type(response_data)}",
                }

            return {
                "status": "success",
                "data": df,
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
            }

    return extract_from_api
