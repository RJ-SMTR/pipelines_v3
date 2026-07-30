{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="aux_verificacao_particao_captura",
    )
}}

with
    ativacao as (
        select distinct
            data, hora, datetime_captura, date(data_fiscalizacao) as data_fiscalizacao
        from {{ ref("staging_fiscalizacao_veiculo_riorotativo") }}
        {% if is_incremental() %}
            where
                (
                    {{
                        generate_date_hour_partition_filter(
                            var("date_range_start"), var("date_range_end")
                        )
                    }}
                )
                and datetime_captura
                between datetime("{{var('date_range_start')}}") and datetime(
                    "{{var('date_range_end')}}"
                )
        {% endif %}
    ),
    particao_completa as (
        select
            data,
            hora,
            datetime_captura,
            array_agg(data_fiscalizacao) as particoes,
            0 as priority
        from ativacao
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
    row_number() over (partition by data, hora, datetime_captura order by priority) = 1
