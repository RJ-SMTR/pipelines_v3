{{
    config(
        alias="transacao_retificada",
    )
}}

select
    data,
    hora,
    id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.id_transacao') as string) as id_transacao,
    replace(
        safe_cast(json_value(content, '$.tipo_transacao_original') as string), '.0', ''
    ) as tipo_transacao_original,
    safe_cast(
        json_value(content, '$.valor_transacao_original') as float64
    ) as valor_transacao_original,
    safe_cast(
        json_value(content, '$.tipo_integracao_original') as string
    ) as tipo_integracao_original,
    safe_cast(
        json_value(content, '$.cd_matriz_integracao_original') as string
    ) as cd_matriz_integracao_original,
    safe_cast(
        json_value(content, '$.uid_origem_original') as string
    ) as uid_origem_original,
    replace(
        safe_cast(json_value(content, '$.tipo_transacao_retificada') as string),
        '.0',
        ''
    ) as tipo_transacao_retificada,
    safe_cast(
        json_value(content, '$.valor_transacao_retificada') as float64
    ) as valor_transacao_retificada,
    safe_cast(
        json_value(content, '$.tipo_integracao_retificada') as string
    ) as tipo_integracao_retificada,
    safe_cast(
        json_value(content, '$.cd_matriz_integracao_retificada') as string
    ) as cd_matriz_integracao_retificada,
    safe_cast(
        json_value(content, '$.uid_origem_retificada') as string
    ) as uid_origem_retificada,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_retificacao') as string)
        ),
        "America/Sao_Paulo"
    ) as data_retificacao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_transacao') as string)
        ),
        "America/Sao_Paulo"
    ) as data_transacao,
from {{ source("source_jae", "transacao_retificada") }}
