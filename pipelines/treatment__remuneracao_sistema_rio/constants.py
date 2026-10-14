# -*- coding: utf-8 -*-
"""Constantes do flow treatment__remuneracao_sistema_rio."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

ADDITIONAL_VARS = {"sistema": "rio"}

REMUNERACAO_SISTEMA_RIO_CHECKS_LIST = {
    "lote_km_rede_referencia": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__lote_km_rede_referencia": {
            "description": "Todos os registros são únicos"
        },
        "dbt_utils__accepted_range__km_referencia_mensal__lote_km_rede_referencia": {
            "description": "Todos os valores da coluna `{column_name}` maiores que zero"
        },
    },
    "servico_oferta_faixa": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__servico_oferta_faixa": {
            "description": "Todos os registros são únicos"
        },
    },
    "aux_viagem_status": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__aux_viagem_status": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagem_valida_temperatura": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__viagem_valida_temperatura": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagem_valida_bilhetagem": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__viagem_valida_bilhetagem": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagem_valida_classificada": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__viagem_valida_classificada": {
            "description": "Todos os registros são únicos"
        },
    },
    "aux_viagem_apurada": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__unique_combination_of_columns__aux_viagem_apurada": {
            "description": "Todos os registros são únicos"
        },
    },
    "viagem_apurada": {
        "dbt_utils__unique_combination_of_columns__viagem_apurada": {
            "description": "Todos os registros são únicos"
        },
    },
    "faixa_apurada": {
        "dbt_utils__unique_combination_of_columns__faixa_apurada": {
            "description": "Todos os registros são únicos"
        },
    },
    "data_apurada": {
        "dbt_utils__unique_combination_of_columns__data_apurada": {
            "description": "Todos os registros são únicos"
        },
    },
    "sumario_servico_sentido_faixa": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "accepted_values": {
            "description": "Todos os valores da coluna `{column_name}` são aceitos"
        },
        "dbt_utils__unique_combination_of_columns__sumario_servico_sentido_faixa": {
            "description": "Todos os registros são únicos"
        },
    },
    "sumario_servico_sentido_dia": {
        "dbt_utils__unique_combination_of_columns__sumario_servico_sentido_dia": {
            "description": "Todos os registros são únicos"
        },
    },
    "sumario_consorcio_quinzena": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils__accepted_range__fator_cumprimento_frota__sumario_consorcio_quinzena": {
            "description": "Todos os valores da coluna `{column_name}` entre 0 e 1, inclusive"
        },
        "dbt_utils__accepted_range": {
            "description": "Todos os valores da coluna `{column_name}` maiores ou iguais a zero"
        },
        "dbt_utils__unique_combination_of_columns__sumario_consorcio_quinzena": {
            "description": "Todos os registros são únicos"
        },
    },
}

REMUNERACAO_SISTEMA_RIO_POST_TEST = DBTTest(
    test_select="path:models/remuneracao_sistema_rio,test_type:data",
    test_descriptions=REMUNERACAO_SISTEMA_RIO_CHECKS_LIST,
    truncate_date=True,
    additional_vars=ADDITIONAL_VARS,
)

REMUNERACAO_SISTEMA_RIO_SELECTOR = DBTSelector(
    name="remuneracao_sistema_rio",
    initial_datetime=datetime(2026, 7, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__remuneracao_sistema_rio",
    post_test=REMUNERACAO_SISTEMA_RIO_POST_TEST,
)
