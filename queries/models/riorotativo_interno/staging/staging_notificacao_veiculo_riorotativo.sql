{{ config(alias="notificacao_veiculo") }}

select
    data,
    hora,
    safe_cast(id as int64) as id_notificacao_veiculo,
    safe_cast(
        json_value(content, '$.id_fiscalizacao_veiculo') as int64
    ) as id_fiscalizacao_veiculo,
    safe_cast(
        json_value(content, '$.id_estacionamento_veiculo') as int64
    ) as id_estacionamento_veiculo,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "notificacao_veiculo") }}
