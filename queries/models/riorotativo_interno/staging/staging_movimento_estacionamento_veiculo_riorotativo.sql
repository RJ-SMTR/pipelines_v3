{{ config(alias="movimento_estacionamento_veiculo") }}

select
    data,
    hora,
    safe_cast(id as int64) as id_movimento_estacionamento_veiculo,
    safe_cast(
        json_value(content, '$.id_estacionamento_veiculo') as int64
    ) as id_estacionamento_veiculo,
    safe_cast(json_value(content, '$.id_tipo_periodo') as int64) as id_tipo_periodo,
    safe_cast(json_value(content, '$.valor_periodo') as numeric) as valor_periodo,
    datetime(
        safe_cast(json_value(content, '$.data_periodo_inicial') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_periodo_inicial,
    datetime(
        safe_cast(json_value(content, '$.data_periodo_final') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_periodo_final,
    safe_cast(json_value(content, '$.id_tipo_pagamento') as int64) as id_tipo_pagamento,
    datetime(
        safe_cast(json_value(content, '$.data_pagamento') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_pagamento,
    safe_cast(json_value(content, '$.valor_pago') as numeric) as valor_pago,
    safe_cast(
        json_value(content, '$.id_notificacao_veiculo') as int64
    ) as id_notificacao_veiculo,
    safe_cast(
        json_value(content, '$.uuid_movimento_estacionamento_veiculo') as string
    ) as uuid_movimento_estacionamento_veiculo,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "movimento_estacionamento_veiculo") }}
