# -*- coding: utf-8 -*-
"""
Tasks para captura de veículos SPPO
"""

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.default_capture.tasks import get_raw_from_gcs
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.veiculo import constants as veiculo_constants
from pipelines.common.utils.extractors.ftp import get_raw_ftp


@task(cache_policy=NO_CACHE)
def create_veiculo_extractor(context: SourceCaptureContext):
    """
    Extrai dados de veículos via FTP RDO com fallback para GCS.

    Tenta buscar do FTP primeiro. Se retornar vazio ou sem dados válidos,
    tenta GCS. Se nenhuma fonte tiver dados, o flow falha.

    Args:
        context (SourceCaptureContext): Contexto de captura. Deve conter
            extra_parameters["ftp_path"] com o caminho base no FTP.

    Returns:
        list[str]: Lista com os caminhos dos arquivos salvos localmente.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado em FTP nem em GCS.
    """
    ftp_path = context.extra_parameters["ftp_path"]
    ftp_full_path = f"{ftp_path}_{context.timestamp.strftime('%Y%m%d')}.txt"

    ftp_result = get_raw_ftp(
        secret_path=veiculo_constants.RDO_FTPS_SECRET_PATH,
        ftp_path=ftp_full_path,
        raw_filepath=context.raw_filepath,
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
