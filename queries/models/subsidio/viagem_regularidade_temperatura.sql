{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

with
    regularidade_temperatura as (
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
            case
                when
                    tipo_viagem not in (
                        "Licenciado com ar e não autuado",
                        "Licenciado sem ar e não autuado"
                    )
                then tipo_viagem
                when indicador_detectado_ar_inoperante
                then "Detectado com ar inoperante"
                else tipo_viagem
            end as tipo_viagem,
            indicador_regularidade_ar_condicionado_viagem,
            indicador_falha_recorrente,
            data_verificacao_falha,
            indicadores,
            servico,
            sentido,
            distancia_planejada
        from {{ ref("eph_viagem_regularidade_temperatura") }}
        where {{ incremental_filter }}
    )
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
    tipo_viagem,
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
    ) as indicadores,
    servico,
    sentido,
    distancia_planejada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from regularidade_temperatura
