{{
    config(
        alias="escola",
    )
}}

select
    data,
    safe_cast(codigo_escola as string) as codigo_escola,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.descricao') as string) as descricao,
    replace(safe_cast(json_value(content, '$.id_cre') as string), '.0', '') as id_cre,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    replace(
        safe_cast(json_value(content, '$.id_rede_ensino') as string), '.0', ''
    ) as id_rede_ensino,
    replace(
        safe_cast(json_value(content, '$.id_escola') as string), '.0', ''
    ) as id_escola
from {{ source("source_jae", "escola") }}
qualify
    row_number() over (partition by codigo_escola order by timestamp_captura desc) = 1
