# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados do STU
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

STU_SOURCE_NAME = "stu"
STU_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-stu-private",
    "dev": "rj-smtr-dev-airbyte",
}

STU_TABLE_CAPTURE_PARAMS = {
    "combustivel": {
        "primary_keys": ["cod_combustivel"],
    },
    "controle_processo": {
        "primary_keys": ["Id"],
    },
    "darm_apropriacao": {
        "primary_keys": ["codrec", "anodarm", "darm", "Emissao"],
    },
    "mod_carroceria": {
        "primary_keys": ["cod_mod_carroceria"],
    },
    "mod_chassi": {
        "primary_keys": ["cod_fab_chassi"],
    },
    "multa": {
        "primary_keys": ["serie", "cm"],
    },
    "permissao": {
        "primary_keys": ["tptran", "tpperm", "termo", "dv"],
    },
    "pessoa_fisica": {
        "primary_keys": ["ratr"],
    },
    "pessoa_juridica": {
        "primary_keys": ["cgc"],
    },
    "planta": {
        "primary_keys": ["planta"],
    },
    "talonario": {
        "primary_keys": ["serie", "cm"],
    },
    "tipo_de_permissao": {
        "primary_keys": ["tptran", "tpperm"],
    },
    "tipo_de_permissionario": {
        "primary_keys": ["tpperm"],
    },
    "tipo_de_transporte": {
        "primary_keys": ["tptran"],
    },
    "tipo_de_veiculo": {
        "primary_keys": ["tpveic"],
    },
    "veiculo": {
        "primary_keys": ["placa"],
    },
    "veiculo_ativo": {
        "primary_keys": ["placa"],
    },
    "vistoria": {
        "primary_keys": ["id_vistoria", "anoexe"],
    },
    "guia": {
        "primary_keys": ["guia"],
        "first_timestamp": datetime(2025, 12, 5, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
    "permis_empresa_escola": {
        "primary_keys": ["tptran", "tpperm", "termo"],
        "first_timestamp": datetime(2025, 12, 5, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
    "modelo": {
        "primary_keys": ["cod_modelo"],
        "first_timestamp": datetime(2025, 12, 5, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
    "multa_postagem": {
        "primary_keys": ["Serie", "CM"],
        "first_timestamp": datetime(2026, 3, 24, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
}

STU_SOURCES = [
    SourceTable(
        source_name=STU_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2025, 10, 9, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__stu_tabelas",
        primary_keys=v["primary_keys"],
        pretreatment_reader_args=v.get("pretreatment_reader_args"),
        pretreat_funcs=v.get("pretreat_funcs"),
        bucket_names=STU_PRIVATE_BUCKET_NAMES,
        partition_date_only=True,
        max_recaptures=4,
        raw_filetype="csv",
    )
    for k, v in STU_TABLE_CAPTURE_PARAMS.items()
]
