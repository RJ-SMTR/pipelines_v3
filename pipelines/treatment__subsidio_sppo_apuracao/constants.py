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
        "staging_gps staging_realocacao gps veiculo_dia tecnologia_servico "
        "viagem_planejada transacao transacao_riocard gps_validador test_completude__temperatura"
    ),
    exclude=(
        "dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas "
        "not_null__data_ordem__transacao "
        "sincronizacao_tabelas__transacao_gratuidade_estudante_municipal"
    ),
    additional_vars={"tipo_teste": "subsidio"},
    truncate_date=True,
    test_descriptions={
        "staging_realocacao": {
            "check_gps_capture__staging_realocacao": {
                "description": "Todos os dados de realocação foram capturados"
            }
        },
        "staging_gps": {
            "check_gps_capture__staging_gps": {
                "description": "Todos os dados de GPS foram capturados"
            }
        },
        "gps": {
            "check_gps_treatment__gps": {
                "description": "Todos os dados de GPS foram devidamente tratados"
            },
            "dbt_utils.unique_combination_of_columns__gps": {
                "description": "Todos os registros são únicos"
            },
        },
        "veiculo_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__data_id_veiculo__veiculo_dia": {
                "description": "Todos os registros são únicos"
            },
            "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__veiculo_dia": {
                "description": "Todas as datas possuem dados"
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
            "dbt_utils.unique_combination_of_columns__viagem_planejada": {
                "description": "Todos os registros são únicos"
            },
            "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart": {
                "description": "Todas as datas possuem dados"
            },
            "accepted_values": {
                "description": "Todos os valores da coluna `{column_name}` são aceitos"
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
        "temperatura": {
            "test_completude__temperatura": {
                "description": "Há pelo menos uma temperatura não nula registrada em cada uma das 24 horas do dia"
            },
        },
        "transacao": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
        "transacao_riocard": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
        "gps_validador": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
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
    initial_datetime=datetime(2026, 5, 5, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

APURACAO_V9_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v9",
    initial_datetime=datetime(2026, 5, 5, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

MONITORAMENTO_SELECTOR = DBTSelector(
    name="monitoramento_subsidio",
    initial_datetime=datetime(2026, 5, 5, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

SNAPSHOT_SUBSIDIO_SELECTOR = DBTSelector(
    name="snapshot_subsidio",
    initial_datetime=datetime(2026, 5, 5, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)
