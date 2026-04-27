# -*- coding: utf-8 -*-
"""
Tasks para captura de veículos SPPO
"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.default_capture.tasks import get_raw_from_gcs
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.veiculo import constants as veiculo_constants
from pipelines.common.utils.extractors.ftp import get_raw_ftp
from pipelines.common.utils.secret import get_env_secret


def get_raw_veiculo_with_fallback(
    secret_path: str,
    ftp_path: str,
    raw_filepath: str,
    context: SourceCaptureContext,
) -> list[str]:
    """
    Busca dados de veículos via FTP RDO com fallback para GCS.

    Tenta buscar do FTP primeiro. Se retornar vazio ou sem dados válidos,
    tenta GCS. Se nenhuma fonte tiver dados, o flow falha.

    Args:
        secret_path (str): Caminho do secret com credenciais FTP
        ftp_path (str): Caminho do arquivo no servidor FTP (sem data)
        raw_filepath (str): Caminho local para salvar o arquivo
        context (SourceCaptureContext): Contexto de captura com timestamp

    Returns:
        list[str]: Lista com os caminhos dos arquivos salvos localmente.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado em FTP nem em GCS.
    """
    ftp_full_path = f"{ftp_path}_{context.timestamp.strftime('%Y%m%d')}.txt"

    credentials = get_env_secret(secret_path)

    ftp_result = get_raw_ftp(
        host=credentials["host"],
        port=int(credentials["port"]),
        username=credentials["username"],
        password=credentials["pwd"],
        ftp_filepaths=[ftp_full_path],
        raw_filepath=raw_filepath,
        raw_filetype="csv",
        encoding="utf-8",
    )

    if ftp_result:
        return ftp_result

    print("[FALLBACK] FTP vazio ou sem dados válidos, tentando GCS...")
    gcs_result = get_raw_from_gcs(
        context=context,
        table_id=context.source.table_id,
    )

    if not gcs_result:
        raise FileNotFoundError(
            f"Arquivo não encontrado em FTP ({ftp_path}) "
            f"nem em GCS para a tabela {context.source.table_id}"
        )

    return gcs_result


@task(cache_policy=NO_CACHE)
def create_veiculo_extractor(context: SourceCaptureContext):
    """
    Cria a extração de dados de veículos via FTP RDO com fallback para GCS.

    Args:
        context (SourceCaptureContext): Contexto de captura contendo informações da fonte

    Returns:
        callable: Função que extrai dados de veículos com fallback
    """
    ftp_path = context.extra_parameters["ftp_path"]

    return partial(
        get_raw_veiculo_with_fallback,
        secret_path=veiculo_constants.RDO_FTPS_SECRET_PATH,
        ftp_path=ftp_path,
        raw_filepath=context.raw_filepath,
        context=context,
    )
