{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

select
    data,
    id_viagem,
    id_veiculo,
    datetime_partida,
    datetime_chegada,
    modo,
    ano_fabricacao,
    tecnologia_apurada,
    tecnologia_remunerada,
    indicador_detectado_ar_inoperante,
    indicador_regularidade_ar_condicionado_viagem,
    json_set(
        json_set(
            json_set(
                json_set(
                    json_set(
                        indicadores,
                        '$.indicador_falha_recorrente.valor',
                        indicador_falha_recorrente
                    ),
                    '$.indicador_falha_recorrente.data_verificacao_falha',
                    data_verificacao_falha
                ),
                '$.indicador_regularidade_ar_condicionado_viagem.valor',
                indicador_regularidade_ar_condicionado_viagem
            ),
            '$.indicador_regularidade_ar_condicionado_viagem.datetime_apuracao_subsidio',
            current_datetime("America/Sao_Paulo")
        ),
        '$.indicador_detectado_ar_inoperante.valor',
        indicador_detectado_ar_inoperante
    ) as indicadores,
    servico,
    sentido,
    distancia_planejada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("eph_viagem_regularidade_temperatura") }}
where {{ incremental_filter }}
