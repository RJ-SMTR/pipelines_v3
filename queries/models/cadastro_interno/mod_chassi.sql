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
    safe_cast(
        replace(
            safe_cast(json_value(content, '$.cod_mod_chassi') as string), '.0', ''
        ) as string
    ) as id_modelo_chassi,
    safe_cast(cod_fab_chassi as string) as id_fabricante,
    safe_cast(json_value(content, '$.des_mod_chassi') as string) as descricao,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "mod_chassi") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
