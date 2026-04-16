{{
    config(
        materialized="view",
    )
}}

with
    -- fmt: off
    source_data as (
        select
            data as data_particao,
            DT_MEDICAO as data_utc,
            concat(
                substr(lpad(safe_cast(HR_MEDICAO as string), 4, '0'), 1, 2),
                ':',
                substr(lpad(safe_cast(HR_MEDICAO as string), 4, '0'), 3, 2),
                ':00'
            ) as hora_utc,
            CD_ESTACAO as id_estacao,
            safe_cast(json_value(content, '$.TEM_INS') as float64) as temperatura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%d %H:%M:%S%Ez',
                    safe_cast(
                        json_value(content, '$._datetime_execucao_flow') as string
                    )
                ),
                "America/Sao_Paulo"
            ) datetime_execucao_flow,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) datetime_captura
        from {{ source("source_inmet", "meteorologia") }}
    ),
    -- fmt: on
    datetime_conversion as (
        select
            *,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S %Z', concat(data_utc, 'T', hora_utc, ' UTC')
                ),
                'America/Sao_Paulo'
            ) as datetime_medicao
        from source_data
    )
select
    data_particao,
    extract(date from datetime_medicao) as data,
    extract(time from datetime_medicao) as hora,
    id_estacao,
    temperatura,
    datetime_execucao_flow,
    datetime_captura
from datetime_conversion
