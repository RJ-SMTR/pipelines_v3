# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors do Rio Rotativo.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__riorotativo_credenciados import (
    constants as riorotativo_credenciados_constants,
)
from pipelines.capture__riorotativo_vagas import constants as riorotativo_vagas_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

RIOROTATIVO_DIARIO_CHECKS_LIST = {
    "guardador_veiculo_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "guardador_veiculo_riorotativo_historico": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
    },
    "agente_verificacao_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "agente_verificacao_riorotativo_historico": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
        "dbt_utils.expression_is_true__agente_verificacao_riorotativo_historico": {
            "description": (
                "Nenhum agente de verificação coincide com guardador de veículo na mesma data"
            )
        },
    },
    "entidade_credenciadora_riorotativo_historico": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.unique_combination_of_columns__entidade_credenciadora_riorotativo_historico": {
            "description": "Cada combinação de CNPJ e data de início de vigência é única"
        },
    },
    "area_estacionamento_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "perfil_funcionamento_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
        "dbt_utils.expression_is_true__perfil_funcionamento_riorotativo": {
            "description": "Todos os dias da semana estão entre 1 e 7"
        },
    },
    "perfil_funcionamento_riorotativo_historico": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique__id_perfil_funcionamento_historico__perfil_funcionamento_riorotativo_historico": {
            "description": "Todos os registros são únicos"
        },
        "dbt_utils.expression_is_true__perfil_funcionamento_riorotativo_historico": {
            "description": "Exatamente um entre id_area e id_perfil_funcionamento é preenchido"
        },
    },
}

RIOROTATIVO_DIARIO_TEST = DBTTest(
    test_select=(
        "guardador_veiculo_riorotativo "
        "guardador_veiculo_riorotativo_historico "
        "agente_verificacao_riorotativo "
        "agente_verificacao_riorotativo_historico "
        "entidade_credenciadora_riorotativo_historico "
        "area_estacionamento_riorotativo "
        "perfil_funcionamento_riorotativo "
        "perfil_funcionamento_riorotativo_historico"
    ),
    test_descriptions=RIOROTATIVO_DIARIO_CHECKS_LIST,
    truncate_date=True,
)

RIOROTATIVO_DIARIO_SELECTOR = DBTSelector(
    name="riorotativo_diario",
    initial_datetime=datetime(2026, 7, 14, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo",
    data_sources=[
        *riorotativo_credenciados_constants.RIOROTATIVO_CREDENCIADOS_SOURCES,
        *riorotativo_vagas_constants.RIOROTATIVO_VAGAS_SOURCES,
    ],
    post_test=RIOROTATIVO_DIARIO_TEST,
)
