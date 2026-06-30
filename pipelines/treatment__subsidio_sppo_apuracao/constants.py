# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do flow treatment__subsidio_sppo_apuracao
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

TZ = ZoneInfo(smtr_constants.TIMEZONE)

DATA_SUBSIDIO_V9_INICIO = datetime(2024, 8, 16, 7, 5, 0, tzinfo=TZ)
DATA_SUBSIDIO_V14_INICIO = datetime(2025, 1, 5, 7, 5, 0, tzinfo=TZ)
SUBSIDIO_INITIAL_DATETIME = datetime(2022, 6, 1, 7, 5, 0, tzinfo=TZ)

INCREMENTAL_DELAY_HOURS = 24 * 7  # D-7

TABLE_IDS_JAE = ["transacao", "transacao_riocard", "gps_validador"]

WEBHOOK_KEY = "subsidio_data_check"

ADDITIONAL_VARS = {"tipo_teste": "subsidio"}

PRE_TEST_SELECT = (
    "sppo_registros sppo_realocacao check_gps_treatment__gps_sppo sppo_veiculo_dia"
    " veiculo_dia tecnologia_servico viagem_planejada transacao transacao_riocard"
    " gps_validador test_completude__temperatura test_jae_captura_subsidio"
)
PRE_TEST_EXCLUDE = (
    "dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas"
    " not_null__data_ordem__transacao"
    " sincronizacao_tabelas__transacao_gratuidade_estudante_municipal"
    " dbt_expectations.expect_row_values_to_have_recent_data__datetime_captura__gps_validador"
)

PRE_CHECKS_LIST = {
    "test_jae_captura_subsidio": {
        "description": (
            "Captura da Jaé sem timestamps ausentes em `transacao`, `transacao_riocard`"
            " e `gps_validador`"
        )
    },
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
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__sppo_veiculo_dia": {
            "description": "Todas as datas possuem dados"
        },
    },
    "veiculo_dia": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__veiculo_dia": {
            "description": "Todas as datas possuem dados"
        },
        "dbt_utils.unique_combination_of_columns__data_id_veiculo__veiculo_dia": {
            "description": "Todos os registros são únicos"
        },
        "test_check_veiculo_lacre__veiculo_dia": {
            "description": "Todos os veículos lacrados têm dados consistentes entre `veiculo_dia` e `veiculo_fiscalizacao_lacre`"
        },
    },
    "tecnologia_servico": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.unique_combination_of_columns__tecnologia_servico": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagem_planejada": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.accepted_range": {
            "description": "Todos os valores da coluna `{column_name}` maiores ou iguais a zero"
        },
        "accepted_values": {
            "description": "Todos os valores da coluna `{column_name}` são aceitos"
        },
        "dbt_utils.unique_combination_of_columns__viagem_planejada": {
            "description": "Todos os registros são únicos"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart": {
            "description": "Todas as datas possuem dados"
        },
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__viagem_planejada": {
            "description": "Todos os dados de `tipo_os` correspondem 1:1 entre as tabelas `subsidio_data_versao_efetiva` e `viagem_planejada`."
        },
        "test_tecnologia_servico_planejado__viagem_planejada": {
            "description": "Todos os serviços planejados possuem tecnologia permitida."
        },
        "check_km_planejada": {
            "description": "Todas as viagens possuem `km_planejada` correspondente à OS"
        },
        "check_partidas_planejadas": {
            "description": "Todas as viagens possuem `partidas_total_planejada` correspondente à OS"
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
    "gps_validador": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
    },
    "transacao": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
    },
    "transacao_riocard": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
    },
}

POST_TEST_V8_SELECT = "dashboard_subsidio_sppo"
POST_TEST_V9_SELECT = "viagens_remuneradas sumario_servico_dia_pagamento valor_km_tipo_viagem"
POST_TEST_V14_SELECT = (
    "viagem_classificada viagem_regularidade_temperatura viagens_remuneradas"
    " sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem"
)

POST_CHECKS_LIST = {
    "sumario_faixa_servico_dia_pagamento": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.accepted_range__km_planejada_faixa__sumario_faixa_servico_dia_pagamento": {
            "description": "Todos os valores da coluna `{column_name}` maiores que zero"
        },
        "dbt_utils.accepted_range": {
            "description": "Todos os valores da coluna `{column_name}` maiores ou iguais a zero"
        },
        "dbt_utils.unique_combination_of_columns__sumario_faixa_servico_dia_pagamento": {
            "description": "Todos os registros são únicos"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__sumario_faixa_servico_dia_pagamento": {
            "description": "Todas as datas possuem dados"
        },
        "check_km_planejada__sumario_faixa_servico_dia_pagamento": {
            "description": "Todas as viagens possuem `km_planejada` correspondente à OS"
        },
        "teto_pagamento_valor_subsidio_pago__sumario_faixa_servico_dia_pagamento": {
            "description": "Todos serviços abaixo do teto de pagamento de valor do subsídio"
        },
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__sumario_faixa_servico_dia_pagamento": {
            "description": "Todos serviços com valores de penalidade aceitos"
        },
        "sumario_servico_dia_tipo_soma_km__km_apurada_dia__sumario_faixa_servico_dia_pagamento": {
            "description": "Todas as somas dos tipos de quilometragem são equivalentes à quilometragem total"
        },
        "expression_is_true__sumario_faixa_servico_dia_pagamento": {
            "description": "Todas as somas de `valor_a_pagar` e `valor_penalidade` não nulas e maiores ou iguais a zero"
        },
    },
    "viagens_remuneradas": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.accepted_range": {
            "description": "Todos os valores da coluna `{column_name}` maiores ou iguais a zero"
        },
        "dbt_utils.unique_combination_of_columns__viagens_remuneradas": {
            "description": "Todas as viagens são únicas"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__viagens_remuneradas": {
            "description": "Todas as datas possuem dados"
        },
        "check_viagem_completa__viagens_remuneradas": {
            "description": "Todas viagens processadas com feed atualizado do GTFS"
        },
        "teto_viagens__viagens_remuneradas": {
            "description": "Todas as viagens foram corretamente identificadas dentro das regras de limite"
        },
    },
    "valor_km_tipo_viagem": {
        "date_overlap_tipo_viagem": {
            "description": "Todos os períodos de vigência não se sobrepõem"
        },
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "viagem_classificada": {
        "dbt_utils.unique_combination_of_columns__viagem_classificada": {
            "description": "Todos os registros são únicos"
        },
        "test_check_tecnologia_minima__viagem_classificada": {
            "description": "Todas as viagens com tecnologia inferior à mínima permitida foram classificadas corretamente"
        },
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "viagem_regularidade_temperatura": {
        "dbt_utils.unique_combination_of_columns__viagem_regularidade_temperatura": {
            "description": "Todos os registros são únicos"
        },
        "test_check_regularidade_temperatura__viagem_regularidade_temperatura": {
            "description": "Todos os registros têm o indicador de regularidade do ar-condicionado consistente com as regras da resolução vigente"
        },
        "dbt_utils.relationships_where__id_viagem__viagem_regularidade_temperatura": {
            "description": "Todos os dados de `id_viagem` correspondem 1:1 entre as tabelas `viagem_classificada` e `viagem_regularidade_temperatura`."
        },
        "test_consistencia_indicadores_temperatura__viagem_regularidade_temperatura": {
            "description": "Todos os registros têm os indicadores de temperatura consistentes entre si."
        },
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
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
