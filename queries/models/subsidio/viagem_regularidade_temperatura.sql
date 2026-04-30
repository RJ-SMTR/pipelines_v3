{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

{% set condicao_veiculo %}
(
    (
        vt.ano_fabricacao <= 2019
        or vt.data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
    )
    and (
        not vt.indicador_temperatura_nula_viagem
        or (
            vt.data >= date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}')
            and coalesce(vr.indicador_falha_recorrente, false)
        )
    )
)
{% endset %}

with
    viagem_temperatura as (
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
            indicadores,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_variacao_viagem.valor'
                ) as bool
            ) as indicador_temperatura_variacao_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_transmitida_viagem.valor'
                ) as bool
            ) as indicador_temperatura_transmitida_viagem,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_temperatura_pos_tratamento_descartada_viagem.valor'
                ) as bool
            ) as indicador_temperatura_pos_tratamento_descartada_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_zero_viagem.valor'
                ) as bool
            ) as indicador_temperatura_zero_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_nula_viagem.valor'
                ) as bool
            ) as indicador_temperatura_nula_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_regular_viagem.valor'
                ) as bool
            ) as indicador_temperatura_regular_viagem,
            servico,
            sentido,
            distancia_planejada
        from {{ ref("aux_viagem_temperatura") }}
        where {{ incremental_filter }}
    ),
    veiculo_regularidade as (
        select
            data,
            id_veiculo,
            indicadores.indicador_falha_recorrente.valor as indicador_falha_recorrente,
            indicadores.indicador_falha_recorrente.data_verificacao_falha
            as data_verificacao_falha
        from {{ ref("veiculo_regularidade_temperatura_dia") }}
        where {{ incremental_filter }}
    ),
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
                    vt.tipo_viagem not in (
                        "Licenciado com ar e não autuado",
                        "Licenciado sem ar e não autuado"
                    )
                then vt.tipo_viagem
                when
                    {{ condicao_veiculo }}
                    and (
                        (
                            vt.data >= date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
                            and coalesce(vr.indicador_falha_recorrente, false)
                        )
                        or vt.indicador_temperatura_zero_viagem
                        or not vt.indicador_temperatura_transmitida_viagem
                        or not vt.indicador_temperatura_regular_viagem
                    )
                then "Detectado com ar inoperante"
                else vt.tipo_viagem
            end as tipo_viagem,
            case
                when {{ condicao_veiculo }}
                then
                    (
                        (
                            vt.data < date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
                            or (
                                vt.data >= date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
                                and not coalesce(vr.indicador_falha_recorrente, false)
                            )
                        )
                        and not vt.indicador_temperatura_zero_viagem
                        and vt.indicador_temperatura_transmitida_viagem
                        and vt.indicador_temperatura_regular_viagem
                    )
                when indicador_temperatura_nula_viagem = true
                then true
                else null
            end as indicador_regularidade_ar_condicionado_viagem,
            indicador_falha_recorrente,
            data_verificacao_falha,
            indicadores,
            servico,
            sentido,
            distancia_planejada
        from viagem_temperatura as vt
        left join veiculo_regularidade as vr using (data, id_veiculo)
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
