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


@task(cache_policy=NO_CACHE)
def create_infracao_extractor_with_fallback(context: SourceCaptureContext):
    """
    Cria função extratora com fallback FTP -> GCS.

    Tenta buscar do FTP primeiro, se falhar tenta GCS.

    Args:
        context (SourceCaptureContext): Contexto de captura

    Returns:
        callable: Função que busca dados com fallback
    """

    def extractor_with_fallback():
        """Tenta FTP primeiro, depois GCS"""
        # Tenta FTP
        ftp_result = create_ftp_extractor(
            context=context,
            ftp_path=constants.SPPO_INFRACAO_FTP_PATH,
            csv_args=constants.SPPO_INFRACAO_CSV_ARGS,
            secret_path=constants.RDO_FTPS_SECRET_PATH,
        )()

        if ftp_result:  # Se conseguiu dados do FTP
            return ftp_result

        print("[FALLBACK] FTP vazio, tentando GCS...")
        # Fallback para GCS
        return get_raw_from_gcs(
            context=context,
            table_id=constants.SPPO_INFRACAO_TABLE_ID,
            csv_args=constants.SPPO_INFRACAO_CSV_ARGS,
        )

    return extractor_with_fallback
