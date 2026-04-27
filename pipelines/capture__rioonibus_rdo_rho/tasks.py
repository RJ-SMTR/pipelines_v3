# -*- coding: utf-8 -*-
"""Tasks de captura dos dados do RDO"""

import re
from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

from dateutil import parser
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__rioonibus_rdo_rho import constants
from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.ftp import get_raw_ftp
from pipelines.common.utils.ftp import connect_ftp
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_rdo_general_extractor(context: SourceCaptureContext):
    """Cria a extração de arquivos do RDO"""

    info = context.source.table_id.split("_")
    transport_mode = info[2].upper()
    report_type = info[0].upper()

    credentials = get_env_secret(constants.RDO_FTPS_SECRET_PATH)
    conetion_params = {
        "host": credentials["host"],
        "port": int(credentials["port"]),
        "username": credentials["username"],
        "password": credentials["pwd"],
    }

    ftp_client = connect_ftp(**conection_params)
    try:
        files_updated_times = {
            file: datetime.timestamp(parser.parse(info["modify"]))
            for file, info in ftp_client.mlsd(transport_mode)
        }
    finally:
        ftp_client.quit()

    files = [
        transport_mode + "/" + filename
        for filename, file_mtime in files_updated_times.items()
        if file_mtime >= datetime(2022, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)).timestamp()
        and "HISTORICO" not in filename
        and filename[:3] == report_type
        and re.findall("2\\d{3}\\d{2}\\d{2}", filename)[-1] == context.timestamp.strftime("%Y%m%d")
    ]

    assert len(files) > 0, (
        f"Sem arquivos {report_type} {transport_mode} {context.timestamp.isoformat()}"
    )

    return partial(
        get_raw_ftp,
        ftp_filepaths=files,
        raw_filetype="csv",
        raw_filepath=context.raw_filepath,
        encoding="latin1",
        **conetion_params,
    )
