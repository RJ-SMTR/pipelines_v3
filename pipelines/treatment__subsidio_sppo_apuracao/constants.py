# -*- coding: utf-8 -*-
"""Valores constantes para materialização dos dados de apuração de subsídio SPPO"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

DATA_SUBSIDIO_V9_INICIO = "2024-08-16"
DATA_SUBSIDIO_V14_INICIO = "2025-01-05"

SUBSIDIO_SPPO_PRE_TEST = DBTTest(
    test_select=(
        "sppo_registros sppo_realocacao check_gps_treatment__gps_sppo sppo_veiculo_dia "
        "veiculo_dia tecnologia_servico viagem_planejada transacao transacao_riocard "
        "gps_validador test_completude__temperatura"
    ),
    exclude=(
        "dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas "
        "not_null__data_ordem__transacao "
        "sincronizacao_tabelas__transacao_gratuidade_estudante_municipal"
    ),
    additional_vars={"tipo_teste": "subsidio"},
    truncate_date=True,
    test_descriptions={
        "sppo_realocacao": {
            "check_gps_capture__sppo_realocacao": {
                "description": "Todos os dados de realocação foram capturados"
            }
        },
        "sppo_registros": {
            "check_gps_capture__sppo_registros": {
                "description": "Todos os dados de GPS foram capturados"
            }
        },
        "gps_sppo": {
            "check_gps_treatment__gps_sppo": {
                "description": "Todos os dados de GPS foram devidamente tratados"
            },
            "dbt_utils.unique_combination_of_columns__gps_sppo": {
                "description": "Todos os registros são únicos"
            },
        },
        "sppo_veiculo_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__data_id_veiculo__sppo_veiculo_dia": {
                "description": "Todos os registros são únicos"
            },
        },
        "veiculo_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "tecnologia_servico": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "viagem_planejada": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
    },
)

SUBSIDIO_SPPO_V8_POS_TEST = DBTTest(
    test_select="dashboard_subsidio_sppo",
    additional_vars={"tipo_teste": "subsidio"},
    truncate_date=True,
)

SUBSIDIO_SPPO_V9_POS_TEST = DBTTest(
    test_select="viagens_remuneradas sumario_servico_dia_pagamento valor_km_tipo_viagem",
    additional_vars={"tipo_teste": "subsidio"},
    truncate_date=True,
    test_descriptions={
        "viagens_remuneradas": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__viagens_remuneradas": {
                "description": "Todas as viagens são únicas"
            },
        },
        "sumario_servico_dia_pagamento": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__sumario_servico_dia_pagamento": {
                "description": "Todos os registros são únicos"
            },
        },
    },
)

SUBSIDIO_SPPO_V14_POS_TEST = DBTTest(
    test_select=(
        "viagem_classificada viagem_regularidade_temperatura viagens_remuneradas "
        "sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem"
    ),
    additional_vars={"tipo_teste": "subsidio"},
    truncate_date=True,
    test_descriptions={
        "viagem_classificada": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__viagem_classificada": {
                "description": "Todos os registros são únicos"
            },
        },
        "viagem_regularidade_temperatura": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "viagens_remuneradas": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "sumario_faixa_servico_dia_pagamento": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
    },
)

APURACAO_V8_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v8",
    initial_datetime=datetime(2024, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

APURACAO_V9_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v9",
    initial_datetime=datetime(2024, 8, 16, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

MONITORAMENTO_SELECTOR = DBTSelector(
    name="monitoramento_subsidio",
    initial_datetime=datetime(2024, 8, 16, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

SNAPSHOT_SUBSIDIO_SELECTOR = DBTSelector(
    name="snapshot_subsidio",
    initial_datetime=datetime(2024, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)
