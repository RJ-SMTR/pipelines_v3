# -*- coding: utf-8 -*-
"""
Utilitários para captura de veículos SPPO
"""

from pipelines.common.capture.default_capture.tasks import (
    create_ftp_extractor,
    get_raw_from_gcs,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.fs import read_raw_data


def create_veiculo_extractor_with_fallback(
    context: SourceCaptureContext,
    ftp_path: str,
    csv_args: dict,
    secret_path: str,
):
    """
    Cria função extratora com fallback FTP -> GCS.

    Tenta buscar do FTP primeiro, se falhar tenta GCS.
    Se nenhuma fonte tiver dados válidos, o flow falha sem criar arquivos vazios.

    Args:
        context (SourceCaptureContext): Contexto de captura
        ftp_path (str): Caminho do arquivo no FTP
        csv_args (dict): Argumentos para leitura do CSV
        secret_path (str): Caminho do secret para autenticação FTP

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
            ftp_path=ftp_path,
            csv_args=csv_args,
            secret_path=secret_path,
        )
        ftp_result = ftp_extractor()

        # Verifica se FTP retornou arquivo vazio (sem dados)
        if ftp_result:
            # Valida se o arquivo contém dados
            data = read_raw_data(
                filepath=ftp_result[0],
                reader_args=csv_args,
            )
            if not data.empty:
                return ftp_result

        print("[FALLBACK] FTP vazio ou sem dados válidos, tentando GCS...")
        # Fallback para GCS
        gcs_result = get_raw_from_gcs(
            context=context,
            table_id=context.source.table_id,
            csv_args=csv_args,
        )

        if not gcs_result:
            raise FileNotFoundError(
                f"Arquivo não encontrado em FTP ({ftp_path}) "
                f"nem em GCS para a tabela {context.source.table_id}"
            )

        return gcs_result

    return extractor_with_fallback
