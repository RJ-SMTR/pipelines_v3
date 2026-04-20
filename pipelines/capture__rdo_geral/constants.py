# -*- coding: utf-8 -*-
"""Valores contantes para captura dos dados do RDO"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__rdo_geral.utils import rename_rdo_columns
from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

RDO_SOURCE_NAME = "rdo"

RDO_FTPS_SECRET_PATH = "smtr_rdo_ftps"

RDO_TABLE_CAPTURE_PARAMS = {
    "rdo_registros_sppo": {},
    # "rdo_registros_stpl": {},
    "rho_registros_sppo": {},
    # "rho_registros_stpl": {},
}

RDO_SOURCES = [
    SourceTable(
        source_name=RDO_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2026, 4, 17, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__jae_auxiliar",
        primary_keys=v.get("primary_keys", []),
        pretreatment_reader_args=v.get(
            "pre_treatment_reader_args",
            {
                "header": None,
                "delimiter": ";",
                "index_col": False,
                "encoding": "latin1",
            },
        ),
        pretreat_funcs=v.get("pretreat_funcs", [rename_rdo_columns]),
        bucket_names=v.get("save_bucket_names"),
        partition_date_only=v.get("partition_date_only", True),
        max_recaptures=v.get("max_recaptures", 10),
        raw_filetype=v.get("raw_filetype", "csv"),
        file_chunk_size=v.get("file_chunk_size"),
    )
    for k, v in RDO_TABLE_CAPTURE_PARAMS.items()
]

RDO_REINDEX_COLUMNS = {
    "SPPO": {
        "RDO": [
            "operadora",
            "linha",
            "data_transacao",
            "tarifa_valor",
            "gratuidade_idoso",
            "gratuidade_especial",
            "gratuidade_estudante_federal",
            "gratuidade_estudante_estadual",
            "gratuidade_estudante_municipal",
            "universitario",
            "gratuito_rodoviario",
            "buc_1a_perna",
            "buc_2a_perna",
            "buc_receita",
            "buc_supervia_1a_perna",
            "buc_supervia_2a_perna",
            "buc_supervia_receita",
            "buc_van_1a_perna",
            "buc_van_2a_perna",
            "buc_van_receita",
            "buc_vlt_1a_perna",
            "buc_vlt_2a_perna",
            "buc_vlt_receita",
            "buc_brt_1a_perna",
            "buc_brt_2a_perna",
            "buc_brt_3a_perna",
            "buc_brt_receita",
            "buc_inter_1a_perna",
            "buc_inter_2a_perna",
            "buc_inter_receita",
            "buc_barcas_1a_perna",
            "buc_barcas_2a_perna",
            "buc_barcas_receita",
            "buc_metro_1a_perna",
            "buc_metro_2a_perna",
            "buc_metro_receita",
            "cartao",
            "receita_cartao",
            "especie_passageiro_transportado",
            "especie_receita",
            "registro_processado",
            "data_processamento",
            "linha_rcti",
        ],
        "RHO": [
            "linha",
            "data_transacao",
            "hora_transacao",
            "total_gratuidades",
            "total_pagantes_especie",
            "total_pagantes_cartao",
            "registro_processado",
            "data_processamento",
            "operadora",
            "linha_rcti",
        ],
    },
    "STPL": {
        "RDO": [
            "operadora",
            "linha",
            "tarifa_valor",
            "data_transacao",
            "gratuidade_idoso",
            "gratuidade_especial",
            "gratuidade_estudante_federal",
            "gratuidade_estudante_estadual",
            "gratuidade_estudante_municipal",
            "universitario",
            "buc_1a_perna",
            "buc_2a_perna",
            "buc_receita",
            "buc_supervia_1a_perna",
            "buc_supervia_2a_perna",
            "buc_supervia_receita",
            "buc_van_1a_perna",
            "buc_van_2a_perna",
            "buc_van_receita",
            "buc_brt_1a_perna",
            "buc_brt_2a_perna",
            "buc_brt_3a_perna",
            "buc_brt_receita",
            "buc_inter_1a_perna",
            "buc_inter_2a_perna",
            "buc_inter_receita",
            "buc_metro_1a_perna",
            "buc_metro_2a_perna",
            "buc_metro_receita",
            "cartao",
            "receita_cartao",
            "especie_passageiro_transportado",
            "especie_receita",
            "registro_processado",
            "data_processamento",
            "linha_rcti",
            "codigo",
        ],
        "RHO": [
            "operadora",
            "linha",
            "data_transacao",
            "hora_transacao",
            "total_gratuidades",
            "total_pagantes",
            "codigo",
        ],
    },
}
