# -*- coding: utf-8 -*-
"""
Tasks para captura de infrações de veículos SPPO
"""

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__veiculo_infracao import constants
from pipelines.common.capture.default_capture.tasks import (
    create_ftp_extractor,
    get_raw_from_gcs,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.fs import read_raw_data


@task(cache_policy=NO_CACHE)
def create_infracao_extractor_with_fallback(context: SourceCaptureContext):
    """
    Cria função extratora com fallback FTP -> GCS.

    Tenta buscar do FTP primeiro, se falhar tenta GCS.
    Se nenhuma fonte tiver dados válidos, o flow falha sem criar arquivos vazios.

    Args:
        context (SourceCaptureContext): Contexto de captura

    Returns:
        callable: Função que busca dados com fallback

    Raises:
        FileNotFoundError: Se arquivo não encontrado em FTP nem em GCS
    """

    def extractor_with_fallback():
        """Tenta FTP primeiro, depois GCS"""
        # Tenta FTP
        ftp_extractor = create_ftp_extractor(
            context=context,
            ftp_path=constants.SPPO_INFRACAO_FTP_PATH,
            csv_args=constants.SPPO_INFRACAO_CSV_ARGS,
            secret_path=constants.RDO_FTPS_SECRET_PATH,
        )
        ftp_result = ftp_extractor()

        # Verifica se FTP retornou arquivo vazio (sem dados)
        if ftp_result:
            # Valida se o arquivo contém dados
            data = read_raw_data(
                filepath=ftp_result[0],
                reader_args=constants.SPPO_INFRACAO_CSV_ARGS,
            )
            if not data.empty:
                return ftp_result

        print("[FALLBACK] FTP vazio ou sem dados válidos, tentando GCS...")
        # Fallback para GCS
        gcs_result = get_raw_from_gcs(
            context=context,
            table_id=constants.SPPO_INFRACAO_TABLE_ID,
            csv_args=constants.SPPO_INFRACAO_CSV_ARGS,
        )

        if not gcs_result:
            raise FileNotFoundError(
                f"Arquivo não encontrado em FTP ({constants.SPPO_INFRACAO_FTP_PATH}) "
                f"nem em GCS para a tabela {constants.SPPO_INFRACAO_TABLE_ID}"
            )

        return gcs_result

    return extractor_with_fallback
