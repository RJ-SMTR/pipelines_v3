# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de vagas do Rio Rotativo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.google_sheets.utils import (
    GoogleSheetTable,
    create_google_sheet_capture_params,
)

RIOROTATIVO_SOURCE_NAME = "riorotativo"
RIOROTATIVO_VAGAS_SPREADSHEET_ID = "1z0KA7kMjsq6visGOt6HTk1jkotKOSCVASp0BxCdt3cM"
RIOROTATIVO_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-riorotativo-private",
    "dev": "rj-smtr-dev-private",
}

RIOROTATIVO_RENAME_MAPPING = {
    "areacodigo": "area_codigo",
    "areanome": "area_nome",
    "arealogradourologradouro": "area_logradouro_logradouro",
    "areaenderecoreferencia": "area_endereco_referencia",
    "areapoligono": "area_poligono",
    "areaobservacao": "area_observacao",
    "areavagatotal": "area_vaga_total",
    "areavagamoto": "area_vaga_moto",
    "areavagaidoso": "area_vaga_idoso",
    "areavagapcd": "area_vaga_pcd",
    "areatempopermanenciahora": "area_tempo_permanencia_hora",
    "areaperfilfuncionamento": "area_perfil_funcionamento",
    "datainiciovigencia": "data_inicio_vigencia",
    "datafimvigencia": "data_fim_vigencia",
    "perfilfuncionamentocodigo": "perfil_funcionamento_codigo",
    "perfilfuncionamentonome": "perfil_funcionamento_nome",
    "perfilfuncionamentodiasemana": "perfil_funcionamento_dia_semana",
    "perfilfuncionamentohorarioinicio": "perfil_funcionamento_horario_inicio",
    "perfilfuncionamentohorariofim": "perfil_funcionamento_horario_fim",
    "perfilfuncionamentoexcecaodatainicio": "perfil_funcionamento_excecao_data_inicio",
    "perfilfuncionamentoexcecaodatafim": "perfil_funcionamento_excecao_data_fim",
    "perfilfuncionamentoexcecaohorarioinicio": "perfil_funcionamento_excecao_horario_inicio",
    "perfilfuncionamentoexcecaohorariofim": "perfil_funcionamento_excecao_horario_fim",
    "perfilfuncionamentoexcecaomotivo": "perfil_funcionamento_excecao_motivo",
    "perfilfuncionamentoexcecaodecisao": "perfil_funcionamento_excecao_decisao",
    "ultimoeditor": "ultimo_editor",
    "ultimaatualizacao": "ultima_atualizacao",
}

RIOROTATIVO_VAGAS_SOURCES, RIOROTATIVO_VAGAS_EXTRA_PARAMETERS = create_google_sheet_capture_params(
    source_name=RIOROTATIVO_SOURCE_NAME,
    flow_folder_name="capture__riorotativo_vagas",
    spread_sheet_id=RIOROTATIVO_VAGAS_SPREADSHEET_ID,
    bucket_names=RIOROTATIVO_PRIVATE_BUCKET_NAMES,
    first_timestamp=datetime(2026, 7, 6, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    rename_mapping=RIOROTATIVO_RENAME_MAPPING,
    tables=[
        GoogleSheetTable(
            table_id="area_estacionamento",
            sheet_name="area_estacionamento",
            primary_keys=["area_codigo", "data_inicio_vigencia"],
        ),
        GoogleSheetTable(
            table_id="perfil_funcionamento",
            sheet_name="perfil_funcionamento",
            primary_keys=["perfil_funcionamento_codigo"],
        ),
        GoogleSheetTable(
            table_id="perfil_funcionamento_excecao",
            sheet_name="perfil_funcionamento_excecao",
            primary_keys=[
                "area_codigo",
                "perfil_funcionamento_codigo",
                "perfil_funcionamento_excecao_decisao",
            ],
        ),
    ],
)
