# -*- coding: utf-8 -*-
import io
from ftplib import FTP
from functools import partial
from typing import Optional, Union

import pandas as pd

from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.ftp import ImplicitFtpTls, connect_ftp

# from pipelines.common import constants as smtr_constants


def get_csv_data_ftp(
    ftp_client: Union[ImplicitFtpTls, FTP],
    save_filepath: str,
    ftp_filepath: str,
    read_raw_params: Optional[dict],
):
    buffer = io.BytesIO()
    ftp_client.retrbinary("RETR " + ftp_filepath, buffer.write)
    buffer.seek(0)
    if read_raw_params is None:
        read_raw_params = {}
    df: pd.DataFrame = pd.read_csv(buffer, **read_raw_params)
    save_local_file(filepath=save_filepath, filetype="csv", data=df)


def get_raw_ftp(  # noqa: PLR0913
    host: str,
    port: int,
    username: str,
    password: str,
    ftp_filepaths: list[str],
    raw_filetype: str,
    raw_filepath: str,
    read_raw_params: Optional[dict] = None,
) -> list[str]:
    if raw_filetype != "csv":
        raise NotImplementedError(f"Captura FPT do tipo {raw_filetype} não implementada!")

    filepaths = []

    ftp_client = connect_ftp(
        host=host,
        port=port,
        username=username,
        password=password,
    )
    try:
        if raw_filetype == "csv":
            get_data_func = partial(
                get_csv_data_ftp,
                ftp_client=ftp_client,
                read_raw_params=read_raw_params,
            )
        else:
            raise NotImplementedError(f"Captura FTP do tipo {raw_filetype} não implementada!")

        page = 0
        for path in ftp_filepaths:
            filepath = raw_filepath.format(page=page)
            get_data_func(ftp_filepath=path, save_filepath=filepath)
            page += 1
            filepaths.append(filepath)

    finally:
        ftp_client.quit()

    return filepaths
