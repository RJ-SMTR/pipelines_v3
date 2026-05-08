{{ config(alias="veiculo_ativo") }}

select
    data,
    safe_cast(placa as string) as placa,
    safe_cast(json_value(content, '$.obs') as string) as obs,
    trim(safe_cast(json_value(content, '$.ordem') as string)) as ordem,
    safe_cast(json_value(content, '$.termo') as string) as termo,
    safe_cast(json_value(content, '$.motivo') as string) as motivo,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.tpperm') as string) as tpperm,
    safe_cast(json_value(content, '$.tptran') as string) as tptran,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S',
            safe_cast(json_value(content, '$.dt_ativo') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_ativo,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S',
            safe_cast(json_value(content, '$.dt_prazo') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_prazo,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "veiculo_ativo") }}
