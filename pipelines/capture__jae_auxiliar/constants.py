# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

JAE_AUXILIAR_SOURCES = [
    SourceTable(
        source_name=jae_constants.JAE_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2024, 1, 7, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__jae_auxiliar",
        primary_keys=v["primary_keys"],
        pretreatment_reader_args=v.get("pre_treatment_reader_args"),
        pretreat_funcs=v.get("pretreat_funcs"),
        bucket_names=v.get("save_bucket_names"),
        partition_date_only=v.get("partition_date_only", True),
        max_recaptures=v.get("max_recaptures", 4),
        raw_filetype=v.get("raw_filetype", "json"),
        file_chunk_size=v.get("file_chunk_size"),
    )
    for k, v in jae_constants.JAE_TABLE_CAPTURE_PARAMS.items()
    if v.get("capture_flow") == "auxiliar"
]
