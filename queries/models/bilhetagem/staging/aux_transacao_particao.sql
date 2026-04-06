{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    transacao as (
        select distinct
            data, hora, timestamp_captura, date(data_transacao) as data_transacao
        from {{ ref("staging_transacao") }}
        {% if is_incremental() %}
            where
                (
                    {{
                        generate_date_hour_partition_filter(
                            var("date_range_start"), var("date_range_end")
                        )
                    }}
                )
                and timestamp_captura
                between datetime("{{var('date_range_start')}}") and datetime(
                    "{{var('date_range_end')}}"
                )
        {% endif %}
    ),
    integracao as (
        select distinct
            data,
            extract(hour from timestamp_captura) as hora,
            timestamp_captura,
            date(data_transacao) as data_transacao
        from
            {{ ref("staging_integracao_transacao") }},
            unnest(
                [
                    data_transacao_t0,
                    data_transacao_t1,
                    data_transacao_t2,
                    data_transacao_t3,
                    data_transacao_t4
                ]
            ) as data_transacao
        where
            data_transacao is not null
            {% if is_incremental() %}
                and date(data) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and timestamp_captura
                between datetime("{{var('date_range_start')}}") and datetime(
                    "{{var('date_range_end')}}"
                )
            {% endif %}
    ),
    novos_dados as (
        select *
        from transacao

        union distinct

        select *
        from integracao
    ),
    particao_completa as (
        select
            data,
            hora,
            timestamp_captura,
            array_agg(data_transacao) as particoes,
            0 as priority
        from novos_dados
        group by 1, 2, 3

        {% if is_incremental() %}
            union all

            select *, 1 as priority
            from {{ this }}
        {% endif %}
    )
select * except (priority)
from particao_completa
qualify
    row_number() over (partition by data, hora, timestamp_captura order by priority) = 1
