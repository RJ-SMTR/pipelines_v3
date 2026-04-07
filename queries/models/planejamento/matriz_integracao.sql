{{
    config(
        materialized="table",
        partition_by={
            "field": "data_inicio",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with
    servicos as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_servico_jae order by data_inicio_vigencia
                    ) as rn
                from {{ ref("servicos") }}
            {# from `rj-smtr.cadastro.servicos` #}
            )
        where rn = 1
    ),
    transferencias as (
        select
            data_inicio,
            data_fim,
            modo_origem,
            id_servico_jae_origem,
            id_servico_gtfs_origem,
            tabela_gtfs_origem,
            integracao_origem,
            modo_destino,
            id_servico_jae_destino,
            id_servico_gtfs_destino,
            tabela_gtfs_destino,
            tempo_integracao_minutos,
            valor_integracao,
            tipo_integracao,
            indicador_integracao
        from {{ ref("aux_matriz_transferencia") }}
    ),
    excecoes as (
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
            if(
                trim(integracao_origem) = '', null, trim(integracao_origem)
            ) as integracao_origem,
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
            cast(valor_integracao as numeric) as valor_integracao,
            tipo_integracao,
            cast(indicador_integracao as bool) as indicador_integracao
        from {{ source("source_smtr", "matriz_integracao_excecao") }}
    ),
    integracoes_regulares as (
        select
            data_inicio,
            data_fim,
            modo_origem,
            id_servico_jae_origem,
            id_servico_gtfs_origem,
            tabela_gtfs_origem,
            integracao_origem,
            modo_destino,
            id_servico_jae_destino,
            id_servico_gtfs_destino,
            tabela_gtfs_destino,
            tempo_integracao_minutos,
            valor_integracao,
            tipo_integracao,
            indicador_integracao
        from {{ ref("aux_matriz_integracao_modo") }}
    ),
    matriz_completa as (
        select *
        from integracoes_regulares

        union distinct

        select *
        from transferencias

        union distinct

        select *
        from excecoes
    )
select
    m.data_inicio,
    m.data_fim,
    concat(
        coalesce(m.integracao_origem, m.modo_origem), '-', m.modo_destino
    ) as integracao,
    m.modo_origem,
    m.id_servico_jae_origem,
    m.id_servico_gtfs_origem,
    m.tabela_gtfs_origem,
    so.servico as servico_origem,
    so.descricao_servico as descricao_servico_origem,
    m.integracao_origem,
    m.modo_destino,
    m.id_servico_jae_destino,
    m.id_servico_gtfs_destino,
    m.tabela_gtfs_destino,
    sd.servico as servico_destino,
    sd.descricao_servico as descricao_servico_destino,
    m.tempo_integracao_minutos,
    m.valor_integracao,
    m.tipo_integracao,
    m.indicador_integracao,
    '{{ var("version") }}' as versao
from matriz_completa m
left join
    servicos so
    on (m.id_servico_jae_origem = so.id_servico_jae)
    or (
        m.id_servico_gtfs_origem = so.id_servico_gtfs
        and m.id_servico_jae_origem is null
    )
left join
    servicos sd
    on (m.id_servico_jae_destino = sd.id_servico_jae)
    or (
        m.id_servico_gtfs_destino = sd.id_servico_gtfs
        and m.id_servico_jae_destino is null
    )
where m.data_inicio < m.data_fim or m.data_fim is null
