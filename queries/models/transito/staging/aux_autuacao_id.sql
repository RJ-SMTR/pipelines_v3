{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_auto_infracao"],
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    date(data)
    between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct concat("'", date(data_autuacao), "'") as partition_date
            from
                (
                    select distinct data_autuacao
                    from {{ ref("autuacao_citran") }}
                    where {{ incremental_filter }}
                    union all
                    select distinct data_autuacao
                    from {{ ref("autuacao_serpro") }}
                    where {{ incremental_filter }}
                )
        {% endset %}
        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    source as (
        select distinct data_autuacao as data, id_auto_infracao, "CITRAN" as fonte
        from {{ ref("autuacao_citran") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        union all
        select distinct data_autuacao as data, id_auto_infracao, "SERPRO" as fonte
        from {{ ref("autuacao_serpro") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    new_data as (
        select s.*
        from source s
        {% if is_incremental() %}
            left join
                {{ this }} t
                on s.data = t.data
                and s.id_auto_infracao = t.id_auto_infracao
                and s.fonte = t.fonte
                and t.data in ({{ partitions | join(", ") }})
            where
                t.id_auto_infracao is null and s.data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    hash_id as (
        select
            data,
            id_auto_infracao,
            to_hex(sha256(concat(generate_uuid(), id_auto_infracao))) as id_autuacao,
            fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from new_data
    ),
    complete_partitions as (
        select *
        from hash_id
        {% if is_incremental() and partitions | length > 0 %}
            union all
            select *
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    )
select *
from complete_partitions
