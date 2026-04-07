{{
    config(
        materialized="ephemeral",
    )
}}


with
    reparticao_unnest as (
        select
            data_inicio_matriz as data_inicio,
            data_fim_matriz as data_fim,
            row_number() over (partition by integracao) as idx_modo,
            integracao,
            modo,
            tempo_integracao_minutos
        from {{ ref("matriz_reparticao_tarifaria") }}, unnest(sequencia_modo) as modo
        where integracao not like "%Metrô%"

    ),
    modos_origem_destino as (
        select
            data_inicio,
            data_fim,
            integracao,
            modo as modo_origem,
            idx_modo,
            string_agg(modo, '-') over (
                partition by integracao order by idx_modo
            ) as integracao_origem,
            lead(modo) over (partition by integracao order by idx_modo) as modo_destino,
            tempo_integracao_minutos
        from reparticao_unnest
    ),

    matriz as (
        select distinct
            data_inicio,
            data_fim,
            case when idx_modo = 1 then modo_origem end as modo_origem,
            cast(null as string) as id_servico_jae_origem,
            cast(null as string) as id_servico_gtfs_origem,
            cast(null as string) as tabela_gtfs_origem,
            case when idx_modo > 1 then integracao_origem end as integracao_origem,
            modo_destino,
            cast(null as string) as id_servico_jae_destino,
            cast(null as string) as id_servico_gtfs_destino,
            cast(null as string) as tabela_gtfs_destino,
            tempo_integracao_minutos,
            'Integração' as tipo_integracao,
            true as indicador_integracao
        from modos_origem_destino
        where modo_destino is not null
    )
select
    case
        when m.data_inicio >= t.data_inicio
        then m.data_inicio
        when m.data_inicio < t.data_inicio
        then t.data_inicio
    end as data_inicio,
    case
        when m.data_fim <= t.data_fim
        then m.data_fim
        when m.data_fim > t.data_fim or m.data_fim is null
        then t.data_fim
    end as data_fim,
    m.modo_origem,
    m.id_servico_jae_origem,
    m.id_servico_gtfs_origem,
    m.tabela_gtfs_origem,
    m.integracao_origem,
    m.modo_destino,
    m.id_servico_jae_destino,
    m.id_servico_gtfs_destino,
    m.tabela_gtfs_destino,
    m.tempo_integracao_minutos,
    t.valor_tarifa as valor_integracao,
    m.tipo_integracao,
    m.indicador_integracao
from matriz m
left join
    {{ ref("tarifa_publica") }} t
    on (t.data_fim >= m.data_inicio or t.data_fim is null)
    and (m.data_fim >= t.data_fim or m.data_fim is null)
