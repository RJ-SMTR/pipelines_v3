{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_cliente_gratuidade",
        partition_by={
            "field": "id_cliente",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000, "interval": 10000},
        },
    )
}}


{% set staging_gratuidade = ref("staging_gratuidade") %}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute and is_incremental() %}
    {% set partitions_query %}
        with
            ids as (
                select distinct cast(cd_cliente as int64) as id
                from {{ staging_gratuidade }}
                where {{ incremental_filter }}
            ),
            grupos as (select distinct div(id, 10000) as group_id from ids),
            identifica_grupos_continuos as (
                select
                    group_id,
                    if(
                        lag(group_id) over (order by group_id) = group_id - 1, 0, 1
                    ) as id_continuidade
                from grupos
            ),
            grupos_continuos as (
                select
                    group_id, sum(id_continuidade) over (order by group_id) as id_continuidade
                from identifica_grupos_continuos
            )
        select
            distinct
            concat(
                "id_cliente between ",
                min(group_id) over (partition by id_continuidade) * 10000,
                " and ",
                (max(group_id) over (partition by id_continuidade) + 1) * 10000 - 1
            )
        from grupos_continuos
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}


with
    gratuidade_complete_partitions as (
        select
            cast(cd_cliente as int64) as id_cliente,
            id as id_gratuidade,
            tipo_gratuidade,
            data_inclusao as datetime_inicio_validade,
            timestamp_captura as datetime_captura,
            0 as priority
        from {{ staging_gratuidade }}
        {% if is_incremental() %}
            where {{ incremental_filter }}
            {% if partitions | length > 0 %}
                union all

                select
                    * except (id_cliente_gratuidade, datetime_fim_validade),
                    1 as priority
                from {{ this }}
                where {{ partitions | join("\nor ") }}
            {% endif %}

        {% endif %}
    ),
    gratuidade_deduplicada as (
        select * except (priority)
        from gratuidade_complete_partitions
        qualify
            row_number() over (
                partition by id_gratuidade, id_cliente
                order by datetime_captura desc, priority
            )
            = 1
    )
select
    id_cliente,
    id_gratuidade,
    concat(id_cliente, '_', id_gratuidade) as id_cliente_gratuidade,
    tipo_gratuidade,
    datetime_inicio_validade,
    lead(datetime_inicio_validade) over (
        partition by id_cliente order by datetime_inicio_validade
    ) as datetime_fim_validade,
    datetime_captura
from gratuidade_deduplicada
