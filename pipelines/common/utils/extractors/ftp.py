# -*- coding: utf-8 -*-
import io
from ftplib import FTP
from typing import Union

from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.ftp import ImplicitFtpTls, connect_ftp

# from pipelines.common import constants as smtr_constants


def get_ftp_data(
    ftp_client: Union[ImplicitFtpTls, FTP],
    ftp_filepath: str,
    encoding: str,
) -> str:
    """
    Obtém o conteúdo de um arquivo no FTP e retorna como string.

    Args:
        ftp_client (Union[ImplicitFtpTls, FTP]): Cliente FTP já conectado.
        ftp_filepath (str): Caminho do arquivo no servidor FTP.
        encoding (str): Encoding utilizado para decodificar o conteúdo.

    Returns:
        str: Conteúdo do arquivo como string decodificada.
    """
    buffer = io.BytesIO()
    ftp_client.retrbinary("RETR " + ftp_filepath, buffer.write)
    buffer.seek(0)

    return buffer.read().decode(encoding)


def get_raw_ftp(  # noqa: PLR0913
    host: str,
    port: int,
    username: str,
    password: str,
    ftp_filepaths: list[str],
    raw_filetype: str,
    raw_filepath: str,
    encoding: str,
) -> list[str]:
    """
    Baixa múltiplos arquivos de um servidor FTP e salva localmente.
    Args:
        host (str): Endereço do servidor FTP.
        port (int): Porta do servidor FTP.
        username (str): Usuário para autenticação.
        password (str): Senha para autenticação.
        ftp_filepaths (list[str]): Lista de caminhos dos arquivos no FTP.
        raw_filetype (str): Tipo do arquivo a ser salvo localmente.
        raw_filepath (str): Template do caminho local com placeholder
            `{page}` para indexação.
        encoding (str): Encoding utilizado para decodificar os arquivos.

    Returns:
        list[str]: Lista com os caminhos dos arquivos salvos localmente.
    """
    filepaths = []

    ftp_client = connect_ftp(
        host=host,
        port=port,
        username=username,
        password=password,
    )
    try:
        page = 0
        for path in ftp_filepaths:
            filepath = raw_filepath.format(page=page)
            page_data = get_ftp_data(
                ftp_client=ftp_client,
                ftp_filepath=path,
                encoding=encoding,
            )

            save_local_file(filepath=filepath, filetype=raw_filetype, data=page_data)
            page += 1
            filepaths.append(filepath)

    finally:
        ftp_client.quit()

    return filepaths
