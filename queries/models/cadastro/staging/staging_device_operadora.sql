{{
    config(
        alias="device_operadora",
    )
}}

select
    data,
    replace(safe_cast(id as string), ".0", "") as id,
    timestamp_captura,
    replace(
        safe_cast(json_value(content, '$.id_device') as string), ".0", ""
    ) as id_device,
    replace(
        safe_cast(json_value(content, '$.cd_operadora_transporte') as string), ".0", ""
    ) as cd_operadora_transporte,
    replace(
        safe_cast(json_value(content, '$.id_grupo_controle_device') as string), ".0", ""
    ) as id_grupo_controle_device,
    safe_cast(json_value(content, '$.apelido') as string) as apelido,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_desassociacao') as string)
        ),
        "America/Sao_Paulo"
    ) as data_desassociacao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        "America/Sao_Paulo"
    ) as data_inclusao,
    safe_cast(json_value(content, '$.nr_serial') as string) as nr_serial,
    replace(
        safe_cast(json_value(content, '$.id_tipo_device') as string), ".0", ""
    ) as id_tipo_device,
from {{ source("source_jae", "device_operadora") }}
