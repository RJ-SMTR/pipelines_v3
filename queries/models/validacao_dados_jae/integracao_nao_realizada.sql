{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set aux_particao_calculo_integracao = ref("aux_particao_calculo_integracao") %}

{% if execute and is_incremental() %}

    {% set partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }} where tipo_particao = 'modificada'

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% set adjacent_partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }} where tipo_particao = 'adjacente'

    {% endset %}

    {% set adjacent_partitions = (
        run_query(adjacent_partitions_query).columns[0].values()
    ) %}

{% endif %}

with
    integracao_transferencia_calculada as (
        select *
        from {{ ref("aux_integracao_calculada") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %}

                    data in ({{ partitions | join(", ") }})
                    or data in ({{ adjacent_partitions | join(", ") }})

                {% else %} false
                {% endif %}
        {% endif %}
        qualify
            max(date(datetime_processamento)) over (partition by id_integracao)
            < current_date("America/Sao_Paulo")
            {% if is_incremental() and partitions | length > 0 %}
                and (
                    date(max(datetime_transacao) over (partition by id_integracao))
                    in ({{ partitions | join(", ") }})
                    or date(min(datetime_transacao) over (partition by id_integracao))
                    in ({{ partitions | join(", ") }})
                )
            {% endif %}
    ),
    integracao_jae as (
        select id_integracao, id_transacao, sequencia_integracao
        from {{ ref("integracao") }}
        where
            {% if is_incremental() %}
                {% if partitions | length > 0 %}

                    data in ({{ partitions | join(", ") }})
                    or data in ({{ adjacent_partitions | join(", ") }})

                {% else %} data = "2000-01-01"
                {% endif %}
            {% else %} data >= "2000-01-01"
            {% endif %}
    ),
    integracao_jae_agg as (
        select
            id_integracao,
            string_agg(id_transacao, '_' order by sequencia_integracao) as id_join
        from integracao_jae
        group by 1
    ),
    particao_completa as (
        select *
        from integracao_transferencia_calculada

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select
                o.* except (
                    classificacao_integracao_nao_realizada,
                    versao,
                    datetime_ultima_atualizacao,
                    id_execucao_dbt
                )
            from {{ this }} o
            left join integracao_transferencia_calculada n using (id_transacao)
            where
                (
                    o.data in ({{ partitions | join(", ") }})
                    or o.data in ({{ adjacent_partitions | join(", ") }})
                )
                and o.id_integracao
                not in (select id_integracao from integracao_transferencia_calculada)
            qualify
                not max(n.id_transacao is not null) over (partition by o.id_integracao)

        {% endif %}
    ),
    integracao_calculada_id_join as (
        select
            id_integracao,
            string_agg(id_transacao, '_' order by sequencia_integracao) as id_join
        from particao_completa
        group by 1
    ),
    integracao_nao_realizada as (
        select
            pc.*,
            case
                when
                    pc.tipo_transacao in ('Integração', 'Transferência')
                    and pc.id_transacao in (select id_transacao from integracao_jae)
                then 'Integração realizada e incompleta na tabela integracao'
                when pc.tipo_transacao in ('Integração', 'Transferência')
                then 'Integração realizada fora da tabela integracao'
                else 'Integração não realizada'
            end as pre_classificacao_transacao
        from particao_completa pc
        join integracao_calculada_id_join id using (id_integracao)
        left join integracao_jae_agg j using (id_join)
        where j.id_join is null
    ),
    integracao_nao_realizada_classificacao_agg as (
        select
            id_integracao,
            array_agg(distinct pre_classificacao_transacao) as array_classificacao
        from integracao_nao_realizada
        group by 1
    ),
    integracao_nao_realizada_classificacao as (
        select
            i.* except (pre_classificacao_transacao),
            case
                when array_length(c.array_classificacao) = 1
                then c.array_classificacao[0]
                when (array_length(c.array_classificacao) = 3)
                then
                    'Integração realizada parcialmente e incompleta na tabela integracao'
                when
                    'Integração não realizada' in unnest(c.array_classificacao)
                    and 'Integração realizada fora da tabela integracao'
                    in unnest(c.array_classificacao)
                then 'Integração realizada parcialmente e fora da tabela integracao'
                when
                    'Integração não realizada' in unnest(c.array_classificacao)
                    and 'Integração realizada e incompleta na tabela integracao'
                    in unnest(c.array_classificacao)
                then 'Integração realizada parcialmente'
                when
                    'Integração realizada fora da tabela integracao'
                    in unnest(c.array_classificacao)
                    and 'Integração realizada e incompleta na tabela integracao'
                    in unnest(c.array_classificacao)
                then 'Integração realizada e incompleta na tabela integracao'
            end as classificacao_integracao_nao_realizada,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from integracao_nao_realizada i
        join integracao_nao_realizada_classificacao_agg c using (id_integracao)
    )
select *
from integracao_nao_realizada_classificacao
