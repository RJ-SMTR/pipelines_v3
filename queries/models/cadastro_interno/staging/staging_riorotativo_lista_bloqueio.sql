{{ config(alias="riorotativo_lista_bloqueio") }}


select
    data,
    safe_cast(cpf as string) as documento,
    "CPF" as tipo_documento,
    safe_cast(json_value(content, '$.motivo_bloqueio') as string) as motivo_bloqueio,
    safe_cast(json_value(content, '$.decisao_bloqueio') as string) as decisao_bloqueio,
    safe.parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.data_inicio_bloqueio') as string)
    ) as data_inicio_bloqueio,
    safe.parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.data_fim_bloqueio') as string)
    ) as data_fim_bloqueio,
    safe_cast(json_value(content, '$.ultimo_editor') as string) as ultimo_editor,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            safe_cast(json_value(content, '$.ultima_atualizacao') as string)
        )
    ) as ultima_atualizacao,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_riorotativo", "lista_bloqueio") }}
qualify
    row_number() over (
        partition by documento, decisao_bloqueio order by datetime_captura desc
    )
    = 1
