# -*- coding: utf-8 -*-
"""Constantes e configuração específicas do tratamento GTFS."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"

GTFS_DISCORD_WEBHOOK = "gtfs"

GTFS_MATERIALIZACAO_DATASET_ID = "gtfs"

PLANEJAMENTO_MATERIALIZACAO_DATASET_ID = "planejamento"

GTFS_DBT_TEST_EXCLUDE = (
    "tecnologia_servico sumario_faixa_servico_dia sumario_faixa_servico_dia_pagamento "
    "viagem_planejada viagens_remuneradas sumario_servico_dia_historico "
    "viagem_planejada_planejamento_dia "
    "planejamento_gtfs_freshness__viagem_planejada_planejamento"
)

GTFS_DBT_TEST_SELECT = (
    f"{GTFS_MATERIALIZACAO_DATASET_ID} "
    f"{PLANEJAMENTO_MATERIALIZACAO_DATASET_ID} "
    "test_consistencia_servicos_ordem_servico_gtfs"
)

GTFS_DATA_CHECKS_LIST = {
    "calendar_gtfs": {
        "dbt_expectations__expect_column_values_to_match_regex__service_id__calendar_gtfs": {
            "description": "Todos os 'service\\_id' começam com 'U\\_', 'S\\_', 'D\\_' ou 'EXCEP'."
        },
    },
    "ordem_servico_trajeto_alternativo_gtfs": {
        "dbt_expectations__expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_gtfs": {
            "description": "Todos os dados de 'feed_start_date' e 'tipo_os' correspondem 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_gtfs' e 'ordem_servico_gtfs'."
        },
    },
    "ordem_servico_trajeto_alternativo_sentido": {
        "dbt_expectations__expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_sentido": {
            "description": "Todos os dados de 'feed_start_date' e 'tipo_os' correspondem 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_sentido' e 'ordem_servico_gtfs'."
        },
        "dbt_expectations__expect_column_values_to_match_regex__evento__ordem_servico_trajeto_alternativo_sentido": {
            "description": "Todos os valores de `evento` em `ordem_servico_trajeto_alternativo_sentido` estão no formato `[a-z0-9_]` (sem acentos ou caracteres especiais)."
        },
        "dbt_utils__relationships_where__servico_evento__ordem_servico_trajeto_alternativo_sentido": {
            "description": "Todos os pares `(servico, evento)` de `ordem_servico_trajeto_alternativo_sentido` constam em `trips_gtfs`."
        },
        "dbt_expectations__expect_table_aggregation_to_equal_other_table__servico_sentido__ordem_servico_trajeto_alternativo_sentido": {
            "description": "A quantidade distinta de 'evento' por 'servico' e 'sentido' corresponde 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_sentido' e 'ordem_servico_trips_shapes_gtfs'."
        },
    },
    "ordem_servico_trips_shapes_gtfs": {
        "dbt_expectations__expect_table_aggregation_to_equal_other_table__ordem_servico_trips_shapes_gtfs": {
            "description": "Todos os dados de 'feed_start_date', 'tipo_os', 'tipo_dia', 'servico' e 'faixa_horaria_inicio' correspondem 1:1 entre as tabelas 'ordem_servico_trips_shapes_gtfs' e 'ordem_servico_faixa_horaria'."
        },
        "dbt_utils__unique_combination_of_columns__ordem_servico_trips_shapes_gtfs": {
            "description": "Todos os dados de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'sentido', 'faixa_horaria_inicio' e 'shape_id' são únicos."
        },
        "dbt_expectations__expect_table_row_count_to_be_between__ordem_servico_trips_shapes_gtfs": {
            "description": "A quantidade de registros de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'faixa_horaria_inicio' e 'shape_id' está dentro do intervalo esperado."
        },
        "dbt_expectations__expect_column_values_to_be_between__distancia_planejada__ordem_servico_trips_shapes_gtfs": {
            "description": "Todos os valores de 'distancia_planejada' são maiores que zero"
        },
    },
    "ordem_servico_faixa_horaria": {
        "dbt_expectations__expect_table_aggregation_to_equal_other_table__ordem_servico_faixa_horaria": {
            "description": "Todos os dados de 'feed_start_date', 'tipo_os', 'tipo_dia', 'servico' e 'faixa_horaria_inicio' correspondem 1:1 entre as tabelas 'ordem_servico_faixa_horaria' e 'ordem_servico_trips_shapes_gtfs'."
        },
    },
    "ordem_servico_faixa_horaria_sentido": {
        "dbt_utils__unique_combination_of_columns__ordem_servico_faixa_horaria_sentido": {
            "description": "Todos os dados de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'sentido' e 'faixa_horaria_inicio' são únicos."
        },
    },
    "trips_gtfs": {
        "test_shape_id_gtfs__trips_gtfs": {
            "description": "Todos os `shape_id` de `trips_gtfs` constam na tabela `shapes_gtfs`"
        },
    },
    "feed_info_gtfs": {
        "unique__feed_start_date__feed_info_gtfs": {
            "description": "Todos os registros de 'feed_start_date' são únicos."
        },
    },
    "viagem_planejada_planejamento": {
        "dbt_utils__unique_combination_of_columns__viagem_planejada_planejamento": {
            "description": "Todos os registros de 'feed_start_date' e 'id_viagem' são únicos."
        },
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__trajetos_alternativos__viagem_planejada_planejamento": {
            "description": (
                "A quantidade distinta de eventos de trajetos alternativos corresponde "
                "entre a Ordem de Serviço e `viagem_planejada_planejamento` para o mesmo "
                "`feed_start_date`, `servico`, `tipo_os` e `sentido`."
            )
        },
    },
    "test_consistencia_servicos_ordem_servico_gtfs": {
        "description": (
            "Todos os serviços presentes na Ordem de Serviço possuem trips, e cada "
            "trip possui frequencies ou stop_times com stop_sequence = 0 para gerar "
            "viagens planejadas."
        )
    },
}


def create_gtfs_selector(data_versao_gtfs: str) -> DBTSelector:
    """Cria o selector com a versão da OS usada pelos testes dbt."""
    return DBTSelector(
        name="gtfs",
        initial_datetime=datetime(2000, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        flow_folder_name="treatment__gtfs",
        post_test=DBTTest(
            test_select=GTFS_DBT_TEST_SELECT,
            exclude=GTFS_DBT_TEST_EXCLUDE,
            test_descriptions=GTFS_DATA_CHECKS_LIST,
            additional_vars={"data_versao_gtfs": data_versao_gtfs},
        ),
    )
