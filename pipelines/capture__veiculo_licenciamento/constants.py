# -*- coding: utf-8 -*-
"""
Valores constantes para captura de licenciamento de veículos SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.veiculo import constants as veiculo_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

# Table IDs
SPPO_LICENCIAMENTO_TABLE_ID = "licenciamento_stu"

# Column mappings from raw data
SPPO_LICENCIAMENTO_MAPPING_KEYS = {
    "placa": "placa",
    "ordem": "id_veiculo",
    "permissao": "permissao",
    "modal": "modo",
    "ultima_vistoria": "data_ultima_vistoria",
    "cod_planta": "id_planta",
    "cod_mod_carroceria": "id_carroceria",
    "cod_fab_carroceria": "id_interno_carroceria",
    "des_mod_carroceria": "carroceria",
    "cod_mod_chassi": "id_chassi",
    "cod_fab_chassi": "id_fabricante_chassi",
    "des_mod_chassi": "nome_chassi",
    "lotacao_sentado": "quantidade_lotacao_sentado",
    "lotacao_pe": "quantidade_lotacao_pe",
    "elevador": "indicador_elevador",
    "ar_condicionado": "indicador_ar_condicionado_stu",
    "tipo_veiculo": "tipo_veiculo",
    "combustivel": "tipo_combustivel",
    "portas": "quantidade_portas",
    "ano_fabricacao": "ano_fabricacao",
    "wifi": "indicador_wifi",
    "usb": "indicador_usb",
    "data_inicio_vinculo": "data_inicio_vinculo",
    "ultima_situacao": "ultima_situacao",
    "ano_ultima_vistoria": "ano_ultima_vistoria",
}

# CSV reading arguments
SPPO_LICENCIAMENTO_CSV_ARGS = {
    "sep": ";",
    "names": SPPO_LICENCIAMENTO_MAPPING_KEYS.values(),
}

# FTP path for raw data
SPPO_LICENCIAMENTO_FTP_PATH = "LICENCIAMENTO/CadastrodeVeiculos"

# RDO FTPS credentials secret path
RDO_FTPS_SECRET_PATH = "smtr_rdo_ftps"


# Dataset and table configuration
SPPO_LICENCIAMENTO_SOURCES = [
    SourceTable(
        source_name=veiculo_constants.SPPO_VEICULO_SOURCE_NAME,
        table_id=SPPO_LICENCIAMENTO_TABLE_ID,
        first_timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        flow_folder_name="capture__veiculo_licenciamento",
        primary_keys=["id_veiculo"],
        pretreatment_reader_args=SPPO_LICENCIAMENTO_CSV_ARGS,
        pretreat_funcs=[],
        raw_filetype="txt",
        partition_date_only=True,
    )
]
