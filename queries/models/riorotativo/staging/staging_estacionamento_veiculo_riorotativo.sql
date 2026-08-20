{{ config(alias="estacionamento_veiculo") }}

select
    data,
    hora,
    replace(safe_cast(id as string), ".0", "") as id_estacionamento_veiculo,
    replace(
        safe_cast(json_value(content, '$.id_veiculo_cliente') as string), ".0", ""
    ) as id_veiculo_cliente,
    safe_cast(json_value(content, '$.latitude') as float64) as latitude,
    safe_cast(json_value(content, '$.longitude') as float64) as longitude,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "estacionamento_veiculo") }}
