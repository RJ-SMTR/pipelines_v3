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
