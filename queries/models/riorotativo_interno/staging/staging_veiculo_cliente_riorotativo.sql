{{ config(alias="veiculo_cliente") }}

select
    data,
    replace(safe_cast(id as string), ".0", "") as id_veiculo_cliente,
    lpad(
        regexp_replace(
            safe_cast(json_value(content, '$.tx_login') as string), r'\D', ''
        ),
        11,
        '0'
    ) as cpf_motorista,
    replace(
        safe_cast(json_value(content, '$.id_veiculo') as string), ".0", ""
    ) as id_veiculo,
    datetime(
        safe_cast(json_value(content, '$.data_inativacao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inativacao,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "veiculo_cliente") }}
