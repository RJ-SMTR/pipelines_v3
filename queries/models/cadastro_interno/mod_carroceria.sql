{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
select
    data,
    safe_cast(cod_mod_carroceria as string) as id_modelo_carroceria,
    safe_cast(
        replace(
            safe_cast(json_value(content, '$.cod_fab_carroceria') as string), '.0', ''
        ) as string
    ) as id_fabricante,
    trim(safe_cast(json_value(content, '$.des_mod_carroceria') as string)) as descricao,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "mod_carroceria") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
