{{ config(alias="combustivel") }}

select
    data,
    safe_cast(cod_combustivel as string) as id_combustivel,
    safe_cast(json_value(content, '$.des_combustivel') as string) as descricao,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "combustivel") }}
