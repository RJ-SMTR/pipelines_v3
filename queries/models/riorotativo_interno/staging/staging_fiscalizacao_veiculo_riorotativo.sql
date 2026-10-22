{{ config(alias="fiscalizacao_veiculo") }}

select
    data,
    hora,
    replace(safe_cast(id as string), ".0", "") as id_fiscalizacao_veiculo,
    replace(
        safe_cast(json_value(content, '$.id_status_fiscalizacao_veiculo') as string),
        ".0",
        ""
    ) as id_status_fiscalizacao_veiculo,
    lpad(
        regexp_replace(
            safe_cast(json_value(content, '$.tx_login') as string), r'\D', ''
        ),
        11,
        '0'
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
    replace(
        safe_cast(json_value(content, '$.id_veiculo') as string), ".0", ""
    ) as id_veiculo,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_fiscalizacao') as string)
        ),
        "America/Sao_Paulo"
    ) as data_fiscalizacao,
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
