{{
    config(
        alias="transacao_erro",
    )
}}

select
    data,
    hora,
    id_transacao_recebida,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.tx_erro') as string) as tx_erro,
    safe_cast(
        json_value(content, '$.tx_complemento_erro') as string
    ) as tx_complemento_erro,
    safe_cast(json_value(content, '$.cd_operadora') as string) as cd_operadora,
    safe_cast(
        json_value(content, '$.nr_logico_midia_operador') as string
    ) as nr_logico_midia_operador,
    safe_cast(json_value(content, '$.id_servico') as string) as id_servico,
    safe_cast(
        json_value(content, '$.id_tipo_transacao') as string
    ) as id_tipo_transacao,
    datetime(
        case
            when length(safe_cast(json_value(content, '$.dt_transacao') as string)) = 26
            then
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E6S',
                    safe_cast(json_value(content, '$.dt_transacao') as string)
                )
            else
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E6S%Ez',
                    safe_cast(json_value(content, '$.dt_transacao') as string)
                )
        end,
        'America/Sao_Paulo'
    ) as dt_transacao,
    safe_cast(json_value(content, '$.tx_transacao') as string) as tx_transacao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.dt_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as dt_inclusao,
from {{ source("source_jae", "transacao_erro") }}
