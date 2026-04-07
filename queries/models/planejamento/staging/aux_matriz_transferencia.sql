{{
    config(
        materialized="ephemeral",
    )
}}

with
    matriz as (
        select
            date(trim(data_inicio)) as data_inicio,
            date(if(trim(data_fim) = '', null, trim(data_fim))) as data_fim,
            modo_origem,
            if(
                trim(id_servico_jae_origem) = '', null, trim(id_servico_jae_origem)
            ) as id_servico_jae_origem,
            if(
                trim(id_servico_gtfs_origem) = '', null, trim(id_servico_gtfs_origem)
            ) as id_servico_gtfs_origem,
            if(
                trim(tabela_gtfs_origem) = '', null, trim(tabela_gtfs_origem)
            ) as tabela_gtfs_origem,
            cast(null as string) as integracao_origem,
            modo_destino,
            if(
                trim(id_servico_jae_destino) = '', null, trim(id_servico_jae_destino)
            ) as id_servico_jae_destino,
            if(
                trim(id_servico_gtfs_destino) = '', null, trim(id_servico_gtfs_destino)
            ) as id_servico_gtfs_destino,
            if(
                trim(tabela_gtfs_destino) = '', null, trim(tabela_gtfs_destino)
            ) as tabela_gtfs_destino,
            cast(tempo_integracao_minutos as float64) as tempo_integracao_minutos,
            'Transferência' as tipo_integracao,
            true as indicador_integracao
        from {{ source("source_smtr", "matriz_transferencia") }}
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
