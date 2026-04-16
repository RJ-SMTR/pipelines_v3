# -*- coding: utf-8 -*-
"""Module to get data from FTP servers"""

from ftplib import FTP
from io import BytesIO

from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.implicit_ftp import ImplicitFtpTls
from pipelines.common.utils.secret import get_env_secret


def connect_ftp(secret_path: str, secure: bool = True):
    """
    Conecta a um servidor FTP.

    Args:
        secret_path (str): Caminho do secret com credenciais FTP
        secure (bool): Se True, usa FTPS implícito (default: True)

    Returns:
        ImplicitFtpTls | FTP: cliente FTP conectado
    """
    ftp_data = get_env_secret(secret_path)
    if secure:
        ftp_client = ImplicitFtpTls()
    else:
        ftp_client = FTP()
    ftp_client.connect(host=ftp_data["host"], port=int(ftp_data["port"]))
    ftp_client.login(user=ftp_data["username"], passwd=ftp_data["pwd"])
    if secure:
        ftp_client.prot_p()
    return ftp_client


def get_ftp_data(
    secret_path: str,
    ftp_path: str,
    secure: bool = True,
) -> str:
    """
    Captura dados de um servidor FTP.

    Args:
        secret_path (str): Caminho do secret com credenciais FTP
        ftp_path (str): Caminho do arquivo no servidor FTP
        secure (bool): Se True, usa FTPS implícito (default: True)

    Returns:
        str: Conteúdo do arquivo como string, ou string vazia se não encontrado
    """
    ftp_client = None
    try:
        ftp_client = connect_ftp(secret_path, secure=secure)
        file_data = BytesIO()
        ftp_client.retrbinary(f"RETR {ftp_path}", file_data.write)
        ftp_client.quit()
        ftp_client = None

        file_data.seek(0)
        return file_data.read().decode("utf-8")

    except (FileNotFoundError, EOFError, OSError) as e:
        print(f"[ERROR] FTP extraction failed: {e}")
        return ""
    finally:
        if ftp_client is not None:
            try:
                ftp_client.quit()
            except OSError:
                pass


def get_raw_ftp(
    secret_path: str,
    ftp_path: str,
    raw_filepath: str,
    secure: bool = True,
) -> list[str]:
    """
    Captura e salva dados de um servidor FTP.

    Args:
        secret_path (str): Caminho do secret com credenciais FTP
        ftp_path (str): Caminho do arquivo no servidor FTP
        raw_filepath (str): Caminho local para salvar o arquivo
        secure (bool): Se True, usa FTPS implícito (default: True)

    Returns:
        list[str]: Lista com o caminho onde os dados foram salvos, ou lista vazia se não encontrado
    """
    csv_content = get_ftp_data(
        secret_path=secret_path,
        ftp_path=ftp_path,
        secure=secure,
    )

    if not csv_content:
        return []

    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype="csv", data=csv_content)

    return [filepath]
