{{
    config(
        materialized="table",
    )
}}

{% set transacao = ref("transacao") %}

with
    particoes as (
        select parse_date("%Y%m%d", partition_id) as particao_date
        from
            `{{ transacao.database }}.{{ transacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ transacao.identifier }}"
            and partition_id != "__NULL__"
            and datetime(
                last_modified_time,
                "America/Sao_Paulo"
            ) between datetime("{{var('date_range_start')}}") and (
                datetime("{{var('date_range_end')}}")
            )
    ),
    particoes_adjacentes as (
        select date_add(particao_date, interval 1 day) as particao_date
        from particoes

        union distinct

        select date_sub(particao_date, interval 1 day) as particao_date
        from particoes
    ),
    union_particoes as (
        select particao_date, concat("'", particao_date, "'") as particao
        from particoes

        union distinct

        select particao_date, concat("'", particao_date, "'") as particao
        from particoes_adjacentes

    ),
    classificacao_particao as (
        select
            u.particao,
            case
                when p.particao_date is not null then "modificada" else "adjacente"
            end as tipo_particao
        from union_particoes u
        left join particoes p using (particao_date)
    )
select *
from classificacao_particao
