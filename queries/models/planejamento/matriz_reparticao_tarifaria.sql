{{
    config(
        materialized="table",
        partition_by={
            "field": "data_inicio_matriz",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    matriz_staging as (
        select
            date(trim(nullif(data_inicio, ''))) as data_inicio_matriz,
            date(trim(nullif(data_fim, ''))) as data_fim_matriz,
            id_tipo_integracao,
            primeira_perna,
            cast(
                if(
                    trim(porcentagem_primeira_perna) = '',
                    null,
                    porcentagem_primeira_perna
                ) as numeric
            )
            / 100 as porcentagem_primeira_perna,
            segunda_perna,
            cast(
                if(
                    trim(porcentagem_segunda_perna) = '',
                    null,
                    porcentagem_segunda_perna
                ) as numeric
            )
            / 100 as porcentagem_segunda_perna,
            terceira_perna,
            cast(
                if(
                    trim(porcentagem_terceira_perna) = '',
                    null,
                    porcentagem_terceira_perna
                ) as numeric
            )
            / 100 as porcentagem_terceira_perna,
            cast(tempo_integracao_minutos as float64) as tempo_integracao_minutos
        from {{ source("source_smtr", "matriz_reparticao_tarifaria") }}
    ),
    matriz as (
        select
            mi.data_inicio_matriz,
            mi.data_fim_matriz,
            mi.id_tipo_integracao as id_matriz_integracao,
            case
                when mi.terceira_perna is not null and trim(mi.terceira_perna) != ''
                then [mi.primeira_perna, mi.segunda_perna, mi.terceira_perna]
                else [mi.primeira_perna, mi.segunda_perna]
            end as sequencia_modo,
            case
                when mi.porcentagem_terceira_perna is not null
                then
                    [
                        mi.porcentagem_primeira_perna,
                        mi.porcentagem_segunda_perna,
                        mi.porcentagem_terceira_perna
                    ]
                else [mi.porcentagem_primeira_perna, mi.porcentagem_segunda_perna]
            end as sequencia_rateio,
            mi.tempo_integracao_minutos
        from matriz_staging mi
    )
select
    data_inicio_matriz,
    data_fim_matriz,
    array_to_string(sequencia_modo, '-') as integracao,
    sequencia_modo,
    sequencia_rateio,
    tempo_integracao_minutos,
    '{{ var("version") }}' as versao
from matriz
