{{ config(alias="fiscalizacao_veiculo") }}

select
    data,
    hora,
    safe_cast(id as int64) as id_fiscalizacao_veiculo,
    safe_cast(
        json_value(content, '$.id_status_fiscalizacao_veiculo') as int64
    ) as id_status_fiscalizacao_veiculo,
    regexp_replace(
        safe_cast(json_value(content, '$.tx_login') as string), r'\D', ''
    ) as cpf_guardador_veiculo,
    safe_cast(json_value(content, '$.latitude') as float64) as latitude,
    safe_cast(json_value(content, '$.longitude') as float64) as longitude,
    upper(
        regexp_replace(
            safe_cast(json_value(content, '$.placa_ocr') as string), r'[^A-Za-z0-9]', ''
        )
    ) as placa_ocr,
    upper(
        regexp_replace(
            safe_cast(json_value(content, '$.placa_digitada') as string),
            r'[^A-Za-z0-9]',
            ''
        )
    ) as placa_digitada,
    safe_cast(json_value(content, '$.id_veiculo') as int64) as id_veiculo,
    safe_cast(
        json_value(content, '$.data_fiscalizacao') as datetime
    ) as datetime_fiscalizacao,
    datetime(
        safe_cast(json_value(content, '$.data_analise') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_analise,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "fiscalizacao_veiculo") }}
