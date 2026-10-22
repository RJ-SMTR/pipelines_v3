{{ config(alias="movimento_estacionamento_veiculo") }}

select
    data,
    hora,
    replace(safe_cast(id as string), ".0", "") as id_movimento_estacionamento_veiculo,
    replace(
        safe_cast(json_value(content, '$.id_estacionamento_veiculo') as string),
        ".0",
        ""
    ) as id_estacionamento_veiculo,
    replace(
        safe_cast(json_value(content, '$.id_tipo_periodo') as string), ".0", ""
    ) as id_tipo_periodo,
    safe_cast(json_value(content, '$.valor_periodo') as numeric) as valor_periodo,
    datetime(
        safe_cast(json_value(content, '$.data_periodo_inicial') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_periodo_inicial,
    datetime(
        safe_cast(json_value(content, '$.data_periodo_final') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_periodo_final,
    replace(
        safe_cast(json_value(content, '$.id_tipo_pagamento') as string), ".0", ""
    ) as id_tipo_pagamento,
    datetime(
        safe_cast(json_value(content, '$.data_pagamento') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_pagamento,
    safe_cast(json_value(content, '$.valor_pago') as numeric) as valor_pago,
    replace(
        safe_cast(json_value(content, '$.id_notificacao_veiculo') as string), ".0", ""
    ) as id_notificacao_veiculo,
    safe_cast(
        json_value(content, '$.uuid_movimento_estacionamento_veiculo') as string
    ) as uuid_movimento_estacionamento_veiculo,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    replace(
        safe_cast(json_value(content, '$.id_veiculo_cliente') as string), ".0", ""
    ) as id_veiculo_cliente,
    safe_cast(json_value(content, '$.latitude') as float64) as latitude,
    safe_cast(json_value(content, '$.longitude') as float64) as longitude,
    safe_cast(json_value(content, '$.area_codigo') as string) as area_codigo,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "movimento_estacionamento_veiculo") }}
