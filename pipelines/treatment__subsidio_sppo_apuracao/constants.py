# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do flow treatment__subsidio_sppo_apuracao
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

TZ = ZoneInfo(smtr_constants.TIMEZONE)

DATA_SUBSIDIO_V9_INICIO = datetime(2024, 8, 16, 0, 0, 0, tzinfo=TZ)
DATA_SUBSIDIO_V14_INICIO = datetime(2025, 1, 5, 0, 0, 0, tzinfo=TZ)
SUBSIDIO_INITIAL_DATETIME = datetime(2022, 6, 1, 0, 0, 0, tzinfo=TZ)

INCREMENTAL_DELAY_HOURS = 24 * 7  # D-7

TABLE_IDS_JAE = []

WEBHOOK_KEY = "subsidio_data_check"

ADDITIONAL_VARS = {"tipo_teste": "subsidio"}

PRE_TEST_SELECT = (
    "sppo_registros sppo_realocacao check_gps_treatment__gps_sppo sppo_veiculo_dia"
    " veiculo_dia tecnologia_servico viagem_planejada transacao transacao_riocard"
    " gps_validador test_completude__temperatura"
)
PRE_TEST_EXCLUDE = (
    "dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas"
    " not_null__data_ordem__transacao"
    " sincronizacao_tabelas__transacao_gratuidade_estudante_municipal"
)

PRE_CHECKS_LIST = {
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
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
        "dbt_utils.unique_combination_of_columns__data_id_veiculo__sppo_veiculo_dia": {
            "description": "Todos os registros são únicos"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__sppo_veiculo_dia": {
            "description": "Todas as datas possuem dados"
        },
    },
    "veiculo_dia": {
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__veiculo_dia": {
            "description": "Todas as datas possuem dados"
        },
        "dbt_utils.unique_combination_of_columns__data_id_veiculo__veiculo_dia": {
            "description": "Todos os registros são únicos"
        },
    },
    "tecnologia_servico": {
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
        "dbt_utils.unique_combination_of_columns__tecnologia_servico": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagem_planejada": {
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
        "dbt_utils.unique_combination_of_columns__viagem_planejada": {
            "description": "Todos os registros são únicos"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart": {
            "description": "Todas as datas possuem dados"
        },
    },
    "temperatura_inmet": {
        "test_completude__temperatura": {
            "description": (
                "Há pelo menos uma temperatura não nula registrada em alguma das"
                " estações do Rio de Janeiro em cada uma das 24 horas do dia"
            )
        },
    },
}

POST_TEST_V8_SELECT = "dashboard_subsidio_sppo"
POST_TEST_V9_SELECT = (
    "viagens_remuneradas sumario_servico_dia_pagamento valor_km_tipo_viagem"
)
POST_TEST_V14_SELECT = (
    "viagem_classificada viagem_regularidade_temperatura viagens_remuneradas"
    " sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem"
)

POST_CHECKS_LIST = {
    "sumario_faixa_servico_dia_pagamento": {
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
        "dbt_utils.unique_combination_of_columns__sumario_faixa_servico_dia_pagamento": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagens_remuneradas": {
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
        "dbt_utils.unique_combination_of_columns__viagens_remuneradas": {
            "description": "Todas as viagens são únicas"
        },
    },
    "valor_km_tipo_viagem": {
        "date_overlap_tipo_viagem": {
            "description": "Todos os períodos de vigência não se sobrepõem"
        },
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
    },
    "viagem_classificada": {
        "dbt_utils.unique_combination_of_columns__viagem_classificada": {
            "description": "Todos os registros são únicos"
        },
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
    },
    "viagem_regularidade_temperatura": {
        "dbt_utils.unique_combination_of_columns__viagem_regularidade_temperatura": {
            "description": "Todos os registros são únicos"
        },
        "not_null": {
            "description": "Todos os valores da coluna `{column_name}` não nulos"
        },
    },
}


APURACAO_SUBSIDIO_PRE_TEST = DBTTest(
    test_select=PRE_TEST_SELECT,
    exclude=PRE_TEST_EXCLUDE,
    test_descriptions=PRE_CHECKS_LIST,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V8_POST_TEST = DBTTest(
    test_select=POST_TEST_V8_SELECT,
    test_descriptions=POST_CHECKS_LIST,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V9_POST_TEST = DBTTest(
    test_select=POST_TEST_V9_SELECT,
    test_descriptions=POST_CHECKS_LIST,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V14_POST_TEST = DBTTest(
    test_select=POST_TEST_V14_SELECT,
    test_descriptions=POST_CHECKS_LIST,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)


APURACAO_SUBSIDIO_V8_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v8",
    initial_datetime=SUBSIDIO_INITIAL_DATETIME,
    final_datetime=DATA_SUBSIDIO_V9_INICIO - timedelta(days=1),
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    post_test=APURACAO_SUBSIDIO_V8_POST_TEST,
)

APURACAO_SUBSIDIO_V9_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v9",
    initial_datetime=DATA_SUBSIDIO_V9_INICIO,
    final_datetime=DATA_SUBSIDIO_V14_INICIO - timedelta(days=1),
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    post_test=APURACAO_SUBSIDIO_V9_POST_TEST,
)

APURACAO_SUBSIDIO_V14_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v14",
    initial_datetime=DATA_SUBSIDIO_V14_INICIO,
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    pre_test=APURACAO_SUBSIDIO_PRE_TEST,
    post_test=APURACAO_SUBSIDIO_V14_POST_TEST,
)

MONITORAMENTO_SUBSIDIO_SELECTOR = DBTSelector(
    name="monitoramento_subsidio",
    initial_datetime=DATA_SUBSIDIO_V9_INICIO,
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

SNAPSHOT_SUBSIDIO_SELECTOR = DBTSelector(
    name="snapshot_subsidio",
    initial_datetime=DATA_SUBSIDIO_V9_INICIO,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)
