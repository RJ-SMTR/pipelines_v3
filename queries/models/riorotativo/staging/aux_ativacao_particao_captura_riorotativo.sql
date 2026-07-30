{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="aux_ativacao_particao_captura",
    )
}}

with
    ativacao as (
        select distinct
            data,
            hora,
            datetime_captura,
            date(datetime_periodo_inicial) as data_periodo_inicial,
            date(datetime_periodo_final) as data_periodo_final
        from {{ ref("staging_movimento_estacionamento_veiculo_riorotativo") }}
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
    dados_novos as (
        select data, hora, datetime_captura, data_periodo_inicial as data_ativacao
        from ativacao

        union distinct

        select data, hora, datetime_captura, data_periodo_final as data_ativacao
        from ativacao
    ),
    particao_completa as (
        select
            data,
            hora,
            datetime_captura,
            array_agg(data_ativacao) as particoes,
            0 as priority
        from dados_novos
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
