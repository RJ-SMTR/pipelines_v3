# -*- coding: utf-8 -*-
"""
Tasks para captura de licenciamento de veículos SPPO
"""

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__veiculo_licenciamento import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.veiculo.utils import (
    create_veiculo_extractor_with_fallback,
)


@task(cache_policy=NO_CACHE)
def create_licenciamento_extractor_with_fallback(context: SourceCaptureContext):
    """
    Cria função extratora com fallback FTP -> GCS para licenciamento.

    Delega para a função genérica de veículo com constantes específicas.
    """
    return create_veiculo_extractor_with_fallback(
        context=context,
        ftp_path=constants.SPPO_LICENCIAMENTO_FTP_PATH,
        csv_args=constants.SPPO_LICENCIAMENTO_CSV_ARGS,
        secret_path=constants.RDO_FTPS_SECRET_PATH,
    )
