{{
    config(
        alias="gratuidade",
    )
}}

select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    replace(
        safe_cast(json_value(content, '$.cd_cliente') as string), '.0', ''
    ) as cd_cliente,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    safe_cast(
        json_value(content, '$.id_status_gratuidade') as string
    ) as id_status_gratuidade,
    safe_cast(
        json_value(content, '$.id_tipo_gratuidade') as string
    ) as id_tipo_gratuidade,
    safe_cast(json_value(content, '$.tipo_gratuidade') as string) as tipo_gratuidade

from {{ source("source_jae", "gratuidade") }}
