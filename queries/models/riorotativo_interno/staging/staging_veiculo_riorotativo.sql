{{ config(alias="veiculo") }}

select
    data,
    hora,
    safe_cast(id as int64) as id_veiculo,
    upper(
        regexp_replace(
            safe_cast(json_value(content, '$.placa') as string), r'[^A-Za-z0-9]', ''
        )
    ) as placa,
    safe_cast(json_value(content, '$.id_modelo_veiculo') as int64) as id_modelo_veiculo,
    safe_cast(json_value(content, '$.cor') as string) as cor,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "veiculo") }}
