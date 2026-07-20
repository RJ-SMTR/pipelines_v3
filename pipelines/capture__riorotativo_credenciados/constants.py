# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de credenciados do Rio Rotativo Digital
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.google_sheets.utils import (
    GoogleSheetTable,
    create_google_sheet_capture_params,
)

RIOROTATIVO_SOURCE_NAME = "riorotativo"
RIOROTATIVO_CREDENCIADOS_SPREADSHEET_ID = "1pAQ59MuY9cLgYfc3pbyg1hH7RCBy1nDDZCSfVAd2DIw"
RIOROTATIVO_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-riorotativo-private",
    "dev": "rj-smtr-dev-private",
}

RIOROTATIVO_RENAME_MAPPING = {
    "motivobloqueio": "motivo_bloqueio",
    "decisaobloqueio": "decisao_bloqueio",
    "datainiciobloqueio": "data_inicio_bloqueio",
    "datafimbloqueio": "data_fim_bloqueio",
    "ultimoeditor": "ultimo_editor",
    "ultimaatualizacao": "ultima_atualizacao",
}

RIOROTATIVO_CREDENCIADOS_SOURCES, RIOROTATIVO_CREDENCIADOS_EXTRA_PARAMETERS = (
    create_google_sheet_capture_params(
        source_name=RIOROTATIVO_SOURCE_NAME,
        flow_folder_name="capture__riorotativo_credenciados",
        spread_sheet_id=RIOROTATIVO_CREDENCIADOS_SPREADSHEET_ID,
        bucket_names=RIOROTATIVO_PRIVATE_BUCKET_NAMES,
        first_timestamp=datetime(2026, 7, 14, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        rename_mapping=RIOROTATIVO_RENAME_MAPPING,
        tables=[
            GoogleSheetTable(
                table_id="entidade_05019730000158",
                sheet_name="05019730000158",
                primary_keys=["cpf"],
                dtypes=str,
                pretreatment_reader_args={"dtype": "object"},
            ),
            GoogleSheetTable(
                table_id="entidade_34152025000122",
                sheet_name="34152025000122",
                primary_keys=["cpf"],
                dtypes=str,
                pretreatment_reader_args={"dtype": "object"},
            ),
            # GoogleSheetTable(
            #     table_id="lista_bloqueio",
            #     sheet_name="lista_bloqueio",
            #     primary_keys=["cpf", "decisao_bloqueio"], considerar adicionar motivo como pk
        ],
    )
)
