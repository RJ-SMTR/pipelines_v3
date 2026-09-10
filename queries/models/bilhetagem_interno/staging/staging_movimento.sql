{{
    config(
        alias="movimento",
    )
}}

select
    data,
    hora,
    id_movimento,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.dt_movimento') as string)
        ),
        "America/Sao_Paulo"
    ) as dt_movimento,
    safe_cast(
        json_value(content, '$.cd_tipo_movimento') as string
    ) as cd_tipo_movimento,
    safe_cast(json_value(content, '$.id_referencia') as string) as id_referencia
from {{ source("source_jae", "movimento") }}
