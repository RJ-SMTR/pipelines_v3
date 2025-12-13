# -*- coding: utf-8 -*-
"""
Valores constantes para pipeline capture__serpro
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

SERPRO_SOURCE_NAME = "serpro"
AUTUACAO_TABLE_ID = "autuacao_v2"

SERPRO_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-infracao-private",
    "dev": "rj-smtr-dev-infracao-private",
}

SERPRO_CAPTURE_PARAMS = {
    "query": """
            SELECT *
            FROM dbpro_radar_view.tb_infracao_view_smtr
            WHERE data_atualizacao_dl = '{data}'
        """,
    "primary_key": ["auinf_num_auto"],
    "save_bucket_names": SERPRO_PRIVATE_BUCKET_NAMES,
    "pre_treatment_reader_args": {"dtype": "object"},
}

SERPRO_AUTUACAO_SOURCE = SourceTable(
    source_name=SERPRO_SOURCE_NAME,
    table_id=AUTUACAO_TABLE_ID,
    first_timestamp=datetime(2025, 12, 15, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    primary_keys=SERPRO_CAPTURE_PARAMS["primary_key"],
    pretreatment_reader_args=SERPRO_CAPTURE_PARAMS["pre_treatment_reader_args"],
    bucket_names=SERPRO_CAPTURE_PARAMS["save_bucket_names"],
    partition_date_only=True,
    raw_filetype="csv",
)

SERPRO_SOURCES = [SERPRO_AUTUACAO_SOURCE]
